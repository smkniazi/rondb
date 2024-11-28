/*
 * Copyright (C) 2024 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */
#include "src/rdrs_rondb_connection_pool.hpp"
#include "src/ttl_purge.hpp"
#include "src/status.hpp"
#include "storage/ndb/plugin/ndb_schema_dist.h"

#include <EventLogger.hpp>
extern EventLogger *g_eventLogger;
#ifdef DEBUG_EVENT
#define DEB_EVENT(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_EVENT(...) do { } while (0)
#endif

TTLPurger::TTLPurger() :
  ndb_(nullptr), exit_(false), cache_updated_(false),
  purge_worker_asks_for_retry_(false),
  schema_watcher_running_(false), schema_watcher_(),
  purge_worker_running_(false), purge_worker_() {
}

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;
TTLPurger::~TTLPurger() {
  exit_ = true;
  if (schema_watcher_running_ && schema_watcher_->joinable()) {
    schema_watcher_->join();
    schema_watcher_ = nullptr;
    schema_watcher_running_ = false;
    if (ndb_) {
      RS_Status status;
      rdrsRonDBConnectionPool->ReturnTTLSchemaWatcherNdbObject(ndb_, &status);
    }
  }
}

bool TTLPurger::Init() {
  RS_Status status = rdrsRonDBConnectionPool->
                       GetTTLSchemaWatcherNdbObject(&ndb_);
  if (status.http_code != SUCCESS) {
    ndb_ = nullptr;
    return false;
  }
  return true;
}

TTLPurger* TTLPurger::CreateTTLPurger() {
  TTLPurger* ttl_purger = new TTLPurger();
  if (!ttl_purger->Init()) {
    delete ttl_purger;
    ttl_purger = nullptr;
  }
  return ttl_purger;
}

static constexpr int NDB_INVALID_SCHEMA_OBJECT = 241;
void TTLPurger::SchemaWatcherJob() {
  bool init_event_succ = false;
  NdbDictionary::Dictionary* dict = nullptr;
  const NdbDictionary::Table* schema_tab = nullptr;
  const NdbDictionary::Table* schema_res_tab = nullptr;
  NdbEventOperation* ev_op = nullptr;
  NdbEventOperation* op = nullptr;
  NdbDictionary::Dictionary::List list;
  const char* message_buf = "API_OK";
  Uint32 event_nums = 0;
  [[maybe_unused]] char event_name_buf[128];
  char slock_buf_pre[32];
  char slock_buf[32];

retry:
  init_event_succ = false;
  dict = nullptr;
  schema_tab = nullptr;
  schema_res_tab = nullptr;
  ev_op = nullptr;
  op = nullptr;
  // Init event
  do {
    if (ndb_ == nullptr) {
      RS_Status status = rdrsRonDBConnectionPool->
                           GetTTLSchemaWatcherNdbObject(&ndb_);
      if (status.http_code != SUCCESS) {
        g_eventLogger->warning("[TTL SWatcher] Failed to get NdbObject. Retry");
        goto err;
      }
    }

    if (ndb_->setDatabaseName(kSystemDBName) != 0) {
      g_eventLogger->warning("[TTL SWatcher] Failed to select system database: "
                            "%s, error: %d(%s). Retry...",
                             kSystemDBName,
                             ndb_->getNdbError().code,
                             ndb_->getNdbError().message);
      goto err;
    }

    dict = ndb_->getDictionary();
    schema_tab = dict->getTable(kSchemaTableName);
    if (schema_tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get system table: %s"
                             ", error: %d(%s). Retry...",
                             kSchemaTableName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }
    schema_res_tab = dict->getTable(kSchemaResTabName);
    if (schema_res_tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get system table: %s"
                             ", error: %d(%s). Retry...",
                             kSchemaResTabName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }

    NdbDictionary::Event my_event(kSchemaEventName);
    my_event.setTable(*schema_tab);
    my_event.addTableEvent(NdbDictionary::Event::TE_ALL);
    my_event.mergeEvents(true);
    my_event.setReportOptions(NdbDictionary::Event::ER_ALL |
                              NdbDictionary::Event::ER_SUBSCRIBE |
                              NdbDictionary::Event::ER_DDL);
    const int n_cols = schema_tab->getNoOfColumns();
    for (int i = 0; i < n_cols; i++) {
      my_event.addEventColumn(i);
    }

    if (dict->createEvent(my_event)) {
      if (dict->getNdbError().classification != NdbError::SchemaObjectExists) {
        g_eventLogger->warning("[TTL SWatcher] Failed to create event"
                               ", error: %d(%s). Retry...",
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
    }
    NdbDictionary::Event_ptr ev(dict->getEvent(kSchemaEventName));
    if (ev) {
      init_event_succ = true;
    } else {
      if (dict->getNdbError().code == NDB_INVALID_SCHEMA_OBJECT &&
          dict->dropEvent(my_event.getName(), 1)) {
        g_eventLogger->warning("[TTL SWatcher] Failed to drop the old event"
                               ", error: %d(%s). Retry...",
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
      g_eventLogger->warning("[TTL SWatcher] Failed to get the event"
                             ", error: %d(%s). "
                             "Dropped the old one and retry...",
                             dict->getNdbError().code,
                             dict->getNdbError().message);
    }
  } while (!exit_ && !init_event_succ);

  // Create event operation
  if ((ev_op = ndb_->createEventOperation(kSchemaEventName)) == nullptr) {
    g_eventLogger->warning("[TTL SWatcher] Failed to create event operation"
                           ", error: %d(%s). Retry...",
                           ndb_->getNdbError().code,
                           ndb_->getNdbError().message);
    goto err;
  }
  ev_op->mergeEvents(true);
  typedef union {
    NdbRecAttr* ra;
    NdbBlob* bh;
  } RA_BH;
  RA_BH rec_attr_pre[kNoEventCol];
  RA_BH rec_attr[kNoEventCol];
  for (int i = 0; i < kNoEventCol; i++) {
    if (i != 3) {
      rec_attr_pre[i].ra = ev_op->getPreValue(kEventColNames[i]);
      rec_attr[i].ra = ev_op->getValue(kEventColNames[i]);
    } else {
      rec_attr_pre[i].bh = ev_op->getPreBlobHandle(kEventColNames[i]);
      rec_attr[i].bh = ev_op->getBlobHandle(kEventColNames[i]);
    }
  }
  if (ev_op->execute()) {
    g_eventLogger->warning("[TTL SWatcher] Failed to execute event operation"
                           ", error: %d(%s). Retry...",
                           ev_op->getNdbError().code,
                           ev_op->getNdbError().message);
    goto err;
  }

  // Fetch tables
  ttl_cache_.clear();
  if (dict->listObjects(list, NdbDictionary::Object::UserTable) != 0) {
    g_eventLogger->warning("[TTL SWatcher] Failed to list objects"
                           ", error: %d(%s). Retry...",
                           dict->getNdbError().code,
                           dict->getNdbError().message);
    goto err;
  }
  for (uint i = 0; i < list.count; i++) {
    NdbDictionary::Dictionary::List::Element& elmt = list.elements[i];

    const char* db_str = elmt.database;
    assert(elmt.schema == std::string("def"));  // always "<db>/def/<name>"
    const char *table_str = elmt.name;
    if (strcmp(db_str, "mysql") == 0) {
      continue;
    }
    if (ndb_->setDatabaseName(db_str) != 0) {
      g_eventLogger->warning("[TTL SWatcher] Failed to select database: %s"
                             ", error: %d(%s). Retry...",
                             db_str,
                             ndb_->getNdbError().code,
                             ndb_->getNdbError().message);
      goto err;
    }
    const NdbDictionary::Table* tab = dict->getTable(
        table_str);
    if (tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get table: %s"
                             ", error: %d(%s). Retry...",
                             table_str,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }
    UpdateLocalCache(db_str, table_str, tab);
  }

  // TODO(Zhao): start purge worker

  // Main schema_watcher_ task
  while (!exit_) {
    int res = ndb_->pollEvents(1000);  // wait for event or 1000 ms
    if (res > 0) {
      while ((op = ndb_->nextEvent())) {
        if (op->hasError()) {
          std::cerr << "Get an event error, " << op->getNdbError().code
                    << "(" << op->getNdbError().message
                    << ") on handling ndb_schema event, retry..."
                    << std::endl;
          g_eventLogger->warning("[TTL SWatcher] Get an event error on "
                                 "handling event"
                                 ", error: %d(%s). Retry...",
                                 op->getNdbError().code,
                                 op->getNdbError().message);
          goto err;
        }
        event_nums++;
        DEB_EVENT("EVENT [%u]: %s, GCI = %llu",
                  event_nums,
                  GetEventName(op->getEventType(), event_name_buf),
                  op->getGCI());
        char* ptr_pre = nullptr;
        char* ptr = nullptr;
        std::string db_str_pre;
        std::string db_str;
        std::string table_str_pre;
        std::string table_str;
        std::string query_str_pre;
        std::string query_str;
        Uint32 node_id = 0;
        Uint32 type = 0;
        [[maybe_unused]] Uint32 id = 0;
        Uint32 schema_op_id = 0;
        NdbTransaction* trans = nullptr;
        NdbOperation* top = nullptr;
        bool clear_slock = false;
        bool trx_succ = false;
        Uint32 trx_failure_times = 0;
        bool cache_updated = false;
        DEB_EVENT("----------------------------");
        switch (op->getEventType()) {
          case NdbDictionary::Event::TE_CLUSTER_FAILURE:
          case NdbDictionary::Event::TE_CREATE:
          case NdbDictionary::Event::TE_ALTER:
          case NdbDictionary::Event::TE_DROP:
          case NdbDictionary::Event::TE_STOP:
          case NdbDictionary::Event::TE_INCONSISTENT:
          case NdbDictionary::Event::TE_OUT_OF_MEMORY:
            // Retry from beginning
            goto err;
          case NdbDictionary::Event::TE_INSERT:
          case NdbDictionary::Event::TE_UPDATE:
          case NdbDictionary::Event::TE_DELETE:
            for (int l = 0; l < kNoEventCol; l++) {
              ptr_pre = rec_attr_pre[l].ra->aRef();
              ptr = rec_attr[l].ra->aRef();
              switch (l) {
                case 0:
                  db_str_pre = std::string(ptr_pre + 1,
                      rec_attr_pre[l].ra->u_8_value());
                  db_str = std::string(ptr + 1,
                      rec_attr[l].ra->u_8_value());
                  DEB_EVENT("  db: %s[%u] -> %s[%u]",
                             db_str_pre.c_str(),
                             rec_attr_pre[l].ra->u_8_value(),
                             db_str.c_str(),
                             rec_attr[l].ra->u_8_value());
                  break;
                case 1:
                  table_str_pre = std::string(ptr_pre + 1,
                      rec_attr_pre[l].ra->u_8_value());
                  table_str = std::string(ptr + 1,
                      rec_attr[l].ra->u_8_value());
                  DEB_EVENT("  table: %s[%u] -> %s[%u]",
                            db_str_pre.c_str(),
                            rec_attr_pre[l].ra->u_8_value(),
                            db_str.c_str(),
                            rec_attr[l].ra->u_8_value());
                  break;
                case 2:
                  {
                  std::string info_buf;
                  memset(slock_buf_pre, 0, 32);
                  memcpy(slock_buf_pre, rec_attr_pre[l].ra->aRef(), 32);
                  info_buf = "  slock: ";
                  for (int i = 0; i < 32; i++) {
                    info_buf += std::to_string(
                                static_cast<unsigned int>(slock_buf_pre[i]));
                    info_buf += " ";
                  }
                  DEB_EVENT("%s", info_buf.c_str());
                  info_buf = "       ->";
                  memset(slock_buf, 0, 32);
                  memcpy(slock_buf, rec_attr[l].ra->aRef(), 32);
                  for (int i = 0; i < 32; i++) {
                    info_buf += std::to_string(
                                static_cast<unsigned int>(slock_buf[i]));
                    info_buf += " ";
                  }
                  DEB_EVENT("%s", info_buf.c_str());
                  }
                  break;
                case 3:
                  {
                    int blob_is_null = 0;
                    Uint64 blob_len = 0;
                    rec_attr_pre[l].bh->getNull(blob_is_null);
                    rec_attr_pre[l].bh->getLength(blob_len);
                    if (blob_is_null == 0 && blob_len != 0) {
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str_pre.resize(read_len, '\0');
                      rec_attr_pre[l].bh->readData(query_str_pre.data(),
                                          read_len);
                      DEB_EVENT("  query: [%llu]%s",
                                blob_len,
                                query_str_pre.c_str());
                    } else {
                      DEB_EVENT("  query: [0]");
                    }
                    DEB_EVENT("       ->");
                    blob_is_null = 0;
                    blob_len = 0;
                    rec_attr[l].bh->getNull(blob_is_null);
                    rec_attr[l].bh->getLength(blob_len);
                    if (blob_is_null == 0 && blob_len != 0) {
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str.resize(read_len, '\0');
                      rec_attr[l].bh->readData(query_str.data(), read_len);
                      DEB_EVENT("         [%llu]%s",
                                blob_len,
                                query_str.c_str());
                    } else {
                      DEB_EVENT("         [0]");
                    }
                    break;
                  }
                case 4:
                  node_id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  node_id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            node_id);
                  break;
                case 5:
                  DEB_EVENT("  epoch: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            rec_attr[l].ra->u_32_value());
                  break;
                case 6:
                  id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            id);
                  break;
                case 7:
                  DEB_EVENT("  version: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            rec_attr[l].ra->u_32_value());
                  break;
                case 8:
                  // SCHEMA_OP_TYPE
                  type = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  type: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            type);
                  break;
                case 9:
                  schema_op_id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  schema_op_id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            schema_op_id);
                  break;
                default:
                  break;
              }
            }
            DEB_EVENT("----------------------------");

            // Check event and update local cache in nessary
            clear_slock = false;
            cache_updated = false;
            switch (type) {
              case SCHEMA_OP_TYPE::SOT_RENAME_TABLE:
                {
                  std::string new_table_str;
                  auto pos = query_str_pre.find(db_str);
                  if (pos != std::string::npos) {
                    pos += db_str.length();
                    assert(query_str_pre.at(pos) == '/');
                    pos += 1;
                    new_table_str = query_str_pre.substr(pos);
                  }
                  if (ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           ndb_->getNdbError().code,
                                           ndb_->getNdbError().message);
                    goto err;
                  }
                  dict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab = dict->getTable(
                      new_table_str.c_str());
                  if (tab == nullptr) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to get table:"
                                           " %s, error: %d(%s). Retry...",
                                           new_table_str.c_str(),
                                           dict->getNdbError().code,
                                           dict->getNdbError().message);
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str,
                                                    new_table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_TABLE:
                {
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str, nullptr);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_DB:
                {
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = DropDBLocalCache(db_str);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CREATE_TABLE:
              case SCHEMA_OP_TYPE::SOT_ALTER_TABLE_COMMIT:
              case SCHEMA_OP_TYPE::SOT_ONLINE_ALTER_TABLE_COMMIT:
                {
                  if (ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           ndb_->getNdbError().code,
                                           ndb_->getNdbError().message);
                    goto err;
                  }
                  dict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab = dict->getTable(
                      table_str.c_str());
                  if (tab == nullptr) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to get table:"
                                           " %s, error: %d(%s). Retry...",
                                           table_str.c_str(),
                                           dict->getNdbError().code,
                                           dict->getNdbError().message);
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CLEAR_SLOCK:
                clear_slock = true;
                break;
              default:
                break;
            }

            // Only purge worker can set cache_updated_ to false;
            if (cache_updated) {
              cache_updated_ = true;
            }

            if (clear_slock) {
              continue;
            }

            trx_succ = false;
            trx_failure_times = 0;
            do {
              trans = ndb_->startTransaction();
              if (trans == nullptr) {
                g_eventLogger->warning("[TTL SWatcher] Failed to start "
                                       "transaction"
                                       ", error: %d(%s). Retry...",
                                       ndb_->getNdbError().code,
                                       ndb_->getNdbError().message);
                goto trx_err;
              }
              top = trans->getNdbOperation(schema_res_tab);
              if (top == nullptr) {
                g_eventLogger->warning("[TTL SWatcher] Failed to get the Ndb "
                                       "operation"
                                       ", error: %d(%s). Retry...",
                                       trans->getNdbError().code,
                                       trans->getNdbError().message);
                goto trx_err;
              }
              if (top->insertTuple() != 0 ||
                  /*Ndb_schema_result_table::COL_NODEID*/
                  top->equal("nodeid", node_id) != 0 ||
                  /*Ndb_schema_result_table::COL_SCHEMA_OP_ID*/
                  top->equal("schema_op_id", schema_op_id) != 0 ||
                  /*Ndb_schema_result_table::COL_PARTICIPANT_NODEID*/
                  top->equal("participant_nodeid", ndb_->getNodeId()) != 0 ||
                  /*Ndb_schema_result_table::COL_RESULT*/
                  top->setValue("result", 0) != 0 ||
                  /*Ndb_schema_result_table::COL_MESSAGE*/
                  top->setValue("message", message_buf) != 0) {
                std::cerr << "Failed to insert tuple, error: "
                          << top->getNdbError().code << "("
                          << top->getNdbError().message << "), retry..."
                          << std::endl;
                g_eventLogger->warning("[TTL SWatcher] Failed to insert tuple "
                                       ", error: %d(%s). Retry...",
                                       top->getNdbError().code,
                                       top->getNdbError().message);
                goto trx_err;
              }
              if (trans->execute(NdbTransaction::Commit,
                    NdbOperation::DefaultAbortOption,
                    1 /*force send*/) != 0) {
                g_eventLogger->warning("[TTL SWatcher] Failed to the execute "
                                       "transaction"
                                       ", error: %d(%s). Retry...",
                                       trans->getNdbError().code,
                                       trans->getNdbError().message);
                goto trx_err;
              } else {
                trx_succ = true;
              }
trx_err:
              if (trans != nullptr) {
                ndb_->closeTransaction(trans);
              }
              if (!trx_succ) {
                trx_failure_times++;
                if (trx_failure_times > 10) {
                  goto err;
                } else {
                  sleep(1);
                }
              }
            } while (!trx_succ);
            break;
          default:
            break;
        }
      }
    } else if (purge_worker_asks_for_retry_) {
      g_eventLogger->warning("[TTL SWatcher] Purge worker asks for retry");
      purge_worker_asks_for_retry_ = false;
      goto err;
    } else if (res < 0) {
      std::cerr << "Failed to poll event, error: "
                << ndb_->getNdbError().code << "("
                << ndb_->getNdbError().message << "), retry..."
                << std::endl;
      g_eventLogger->warning("[TTL SWatcher] Failed to poll event "
                             ", error: %d(%s). Retry...",
                             ndb_->getNdbError().code,
                             ndb_->getNdbError().message);
      goto err;
    }
  }
err:
  if (ev_op != nullptr) {
    ndb_->dropEventOperation(ev_op);
  }
  ev_op = nullptr;
  op = nullptr;
  if (dict != nullptr) {
    dict->dropEvent(kSchemaEventName);
  }
  // TODO(Zhao): stop purge worker
  RS_Status status;
  rdrsRonDBConnectionPool->ReturnTTLSchemaWatcherNdbObject(ndb_, &status);
  ndb_ = nullptr;
  if (!exit_) {
    sleep(2);
    goto retry;
  }
  return;
}

bool TTLPurger::UpdateLocalCache(const std::string& db,
                                 const std::string& table,
                                 const NdbDictionary::Table* tab) {
  bool updated = false;
  auto iter = ttl_cache_.find(db + "/" + table);
  if (tab != nullptr) {
    if (iter != ttl_cache_.end()) {
      if (tab->isTTLEnabled()) {
        assert(iter->second.table_id == tab->getTableId());
        std::cerr << "Update TTL of table " << db + "/" + table
                  << " in cache: [" << iter->second.table_id
                  << ", " << iter->second.ttl_sec
                  << ", " << iter->second.col_no
                  << "] -> [" << tab->getTableId()
                  << ", " << tab->getTTLSec()
                  << ", " << tab->getTTLColumnNo()
                  << "]" << std::endl;
        g_eventLogger->info("[TTL SWatcher] Update TTL of table %s.%s "
                            "in cache: [%u, %u@%u] -> [%u, %u@%u]",
                            db.c_str(), table.c_str(),
                            iter->second.table_id, iter->second.ttl_sec,
                            iter->second.col_no,
                            tab->getTableId(), tab->getTTLSec(),
                            tab->getTTLColumnNo());
        iter->second.ttl_sec = tab->getTTLSec();
        iter->second.col_no = tab->getTTLColumnNo();
      } else {
        g_eventLogger->info("[TTL SWatcher] Remove[1] TTL of table %s.%s "
                             "in cache: [%u, %u@%u]",
                             db.c_str(), table.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        ttl_cache_.erase(iter);
      }
      updated = true;
    } else {
      if (tab->isTTLEnabled()) {
        g_eventLogger->info("[TTL SWatcher] Insert TTL of table %s.%s "
                             "in cache: [%u, %u@%u]",
                             db.c_str(), table.c_str(), tab->getTableId(),
                             tab->getTTLSec(), tab->getTTLColumnNo());
        ttl_cache_.insert({db + "/" + table, {tab->getTableId(),
                           tab->getTTLSec(), tab->getTTLColumnNo()}});
        updated = true;
      } else {
        // check mysql.ttl_purge_nodes
        // TODO(zhao): handle ttl_purge_tables as well
        if (db == kSystemDBName && table == kTTLPurgeNodesTabName) {
          updated = true;
        }
      }
    }
  } else {
    if (iter != ttl_cache_.end()) {
      std::cerr << "Remove[2] TTL of table " << db + "/" + table
                << " in cache: [" << iter->second.table_id
                << ", " << iter->second.ttl_sec
                << ", " << iter->second.col_no
                << "]" << std::endl;
      g_eventLogger->info("[TTL SWatcher] Remove[2] TTL of table %s.%s "
                           "in cache: [%u, %u@%u]",
                           db.c_str(), table.c_str(), iter->second.table_id,
                           iter->second.ttl_sec, iter->second.col_no);
      ttl_cache_.erase(iter);
      updated = true;
    } else {
      // check mysql.ttl_purge_nodes
      // TODO(zhao): handle ttl_purge_tables as well
      if (db == kSystemDBName && table == kTTLPurgeNodesTabName) {
        updated = true;
      }
    }
  }
  return updated;
}

bool TTLPurger::UpdateLocalCache(const std::string& db,
                                 const std::string& table,
                                 const std::string& new_table,
                                 const NdbDictionary::Table* tab) {
  // 1. Remove old table
  bool ret = UpdateLocalCache(db, table, nullptr);
  assert(ret);
  // 2. Insert new table
  ret = UpdateLocalCache(db, new_table, tab);
  assert(ret);
  return ret;
}

char* TTLPurger::GetEventName(NdbDictionary::Event::TableEvent event_type,
                              char* name_buf) {
  switch (event_type) {
    case NdbDictionary::Event::TE_INSERT:
      strcpy(name_buf, "TE_INSERT");
      break;
    case NdbDictionary::Event::TE_DELETE:
      strcpy(name_buf, "TE_DELETE");
      break;
    case NdbDictionary::Event::TE_UPDATE:
      strcpy(name_buf, "TE_UPDATE");
      break;
    case NdbDictionary::Event::TE_SCAN:
      strcpy(name_buf, "TE_SCAN");
      break;
    case NdbDictionary::Event::TE_DROP:
      strcpy(name_buf, "TE_DROP");
      break;
    case NdbDictionary::Event::TE_ALTER:
      strcpy(name_buf, "TE_ALTER");
      break;
    case NdbDictionary::Event::TE_CREATE:
      strcpy(name_buf, "TE_CREATE");
      break;
    case NdbDictionary::Event::TE_GCP_COMPLETE:
      strcpy(name_buf, "TE_GCP_COMPLETE");
      break;
    case NdbDictionary::Event::TE_CLUSTER_FAILURE:
      strcpy(name_buf, "TE_CLUSTER_FAILURE");
      break;
    case NdbDictionary::Event::TE_STOP:
      strcpy(name_buf, "TE_STOP");
      break;
    case NdbDictionary::Event::TE_NODE_FAILURE:
      strcpy(name_buf, "TE_NODE_FAILURE");
      break;
    case NdbDictionary::Event::TE_SUBSCRIBE:
      strcpy(name_buf, "TE_SUBSCRIBE");
      break;
    case NdbDictionary::Event::TE_UNSUBSCRIBE:
      strcpy(name_buf, "TE_UNSUBSCRIBE");
      break;
    case NdbDictionary::Event::TE_EMPTY:
      strcpy(name_buf, "TE_EMPTY");
      break;
    case NdbDictionary::Event::TE_INCONSISTENT:
      strcpy(name_buf, "TE_INCONSISTENT");
      break;
    case NdbDictionary::Event::TE_OUT_OF_MEMORY:
      strcpy(name_buf, "TE_OUT_OF_MEMEORY");
      break;
    case NdbDictionary::Event::TE_ALL:
      strcpy(name_buf, "TE_ALL");
      break;
    default:
      strcpy(name_buf, "UNKNOWN");
      break;
  }
  return name_buf;
}

bool TTLPurger::DropDBLocalCache(const std::string& db_str) {
  bool updated = false;
  for (auto iter = ttl_cache_.begin(); iter != ttl_cache_.end();) {
    auto pos = iter->first.find('/');
    if (pos != std::string::npos) {
      std::string db = iter->first.substr(0, pos);
      if (db == db_str) {
        g_eventLogger->info("[TTL SWatcher] Remove[3] TTL of table %s "
                             "in cache: [%u, %u@%u]",
                             iter->first.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        iter = ttl_cache_.erase(iter);
        updated = true;
        continue;
      }
    }
    iter++;
  }
  return updated;
}

bool TTLPurger::Run() {
  if (!schema_watcher_running_) {
    assert(schema_watcher_ == nullptr);
    assert(!purge_worker_running_);
    schema_watcher_ = new std::thread(
                      std::bind(&TTLPurger::SchemaWatcherJob, this));
    schema_watcher_running_ = true;
  }
  return true;
}
