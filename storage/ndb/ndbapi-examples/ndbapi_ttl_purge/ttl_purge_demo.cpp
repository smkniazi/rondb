/*
   Copyright (c) 2005, 2024, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
 *  ndbapi_event.cpp: Using API level events in NDB API
 *
 *  Classes and methods used in this example:
 *
 *  Ndb_cluster_connection
 *       connect()
 *       wait_until_ready()
 *
 *  Ndb
 *       init()
 *       getDictionary()
 *       createEventOperation()
 *       dropEventOperation()
 *       pollEvents()
 *       nextEvent()
 *
 *  NdbDictionary
 *       createEvent()
 *       dropEvent()
 *
 *  NdbDictionary::Event
 *       setTable()
 *       addTableEvent()
 *       addEventColumn()
 *
 *  NdbEventOperation
 *       getValue()
 *       getPreValue()
 *       execute()
 *       getEventType()
 *
 */

#include <NdbApi.hpp>

#include <stdlib.h>
#include <string.h>
// Used for cout
#include <stdio.h>
#include <iostream>
#ifdef VM_TRACE
#include <my_dbug.h>
#endif
#ifndef assert
#include <assert.h>
#endif

#include <atomic>
#include <thread>
#include "storage/ndb/plugin/ndb_schema_dist.h"
#include "mysql_time.h"
#include "my_systime.h"
#include "myisampack.h"
#define APIERROR(error)                                                       \
  {                                                                           \
    std::cout << "Error in " << __FILE__ << ", line:" << __LINE__             \
              << ", code:" << error.code << ", msg: " << error.message << "." \
              << std::endl;                                                   \
    exit(-1);                                                                 \
  }

constexpr int NDB_INVALID_SCHEMA_OBJECT = 241;

char* GetEventName(NdbDictionary::Event::TableEvent event_type,
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

bool IsExit() {
  return false;
}

typedef struct {
  int32_t table_id;
  uint32_t ttl_sec;
  uint32_t col_no;
  uint32_t part_id = {0}; // Only valid in local ttl cache
  char last_purged[8] = {0}; // Only valid in local ttl cache
} TTLInfo;

std::mutex g_mutex;
std::atomic<bool> g_cache_updated;
std::map<std::string, TTLInfo> g_ttl_cache;
bool UpdateLocalCache(const std::string& db,
                      const std::string& table,
                      const NdbDictionary::Table* tab) {
  bool updated = false;
  auto iter = g_ttl_cache.find(db + "/" + table);
  if (tab != nullptr) {
    if (iter != g_ttl_cache.end()) {
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
        iter->second.ttl_sec = tab->getTTLSec();
        iter->second.col_no = tab->getTTLColumnNo();
      } else {
        std::cerr << "Remove[1] TTL of table " << db + "/" + table
                  << " in cache: [" << iter->second.table_id
                  << ", " << iter->second.ttl_sec
                  << ", " << iter->second.col_no
                  << "]" << std::endl;
        g_ttl_cache.erase(iter);
      }
      updated = true;
    } else {
      if (tab->isTTLEnabled()) {
        std::cerr << "Insert TTL of table " << db + "/" + table
                  << " in cache: [" << tab->getTableId()
                  << ", " << tab->getTTLSec()
                  << ", " << tab->getTTLColumnNo()
                  << "]" << std::endl;
        g_ttl_cache.insert({db + "/" + table, {tab->getTableId(),
                           tab->getTTLSec(), tab->getTTLColumnNo()}});
        updated = true;
      } else {
        // check mysql.ttl_purge_nodes
        // TODO(zhao): handle ttl_purge_tables as well
        if (db == "mysql" && table == "ttl_purge_nodes") {
          updated = true;
        }
      }
    }
  } else {
    if (iter != g_ttl_cache.end()) {
      std::cerr << "Remove[2] TTL of table " << db + "/" + table
                << " in cache: [" << iter->second.table_id
                << ", " << iter->second.ttl_sec
                << ", " << iter->second.col_no
                << "]" << std::endl;
      g_ttl_cache.erase(iter);
      updated = true;
    } else {
      // check mysql.ttl_purge_nodes
      // TODO(zhao): handle ttl_purge_tables as well
      if (db == "mysql" && table == "ttl_purge_nodes") {
        updated = true;
      }
    }
  }
  return updated;
}

bool UpdateLocalCache(const std::string& db,
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

bool DropDBLocalCache(std::string& db_str) {
  bool updated = false;
  for (auto iter = g_ttl_cache.begin(); iter != g_ttl_cache.end();) {
    auto pos = iter->first.find('/');
    if (pos != std::string::npos) {
      std::string db = iter->first.substr(0, pos);
      if (db == db_str) {
        std::cerr << "Remove[3] TTL of table " << iter->first
                  << " in cache: [" << iter->second.table_id
                  << ", " << iter->second.ttl_sec
                  << ", " << iter->second.col_no
                  << "]" << std::endl;
        iter = g_ttl_cache.erase(iter);
        updated = true;
        continue;
      }
    }
    iter++;
  }
  return updated;
}

longlong Datetime2ll(const MYSQL_TIME &my_time) {
  const longlong ymd = ((my_time.year * 13 + my_time.month) << 5) | my_time.day;
  const longlong hms =
      (my_time.hour << 12) | (my_time.minute << 6) | my_time.second;
  assert(std::abs(static_cast<longlong>(my_time.second_part)) <= 0xffffffLL);
  const longlong tmp = (static_cast<ulonglong>((ymd << 17) | hms) << 24)
                       + my_time.second_part;
  return my_time.neg ? -tmp : tmp;
}

void LL2datetime(MYSQL_TIME *ltime, int64_t tmp) {
  int64_t ymd;
  int64_t hms;
  int64_t ymdhms;
  int64_t ym;

  if ((ltime->neg = (tmp < 0))) tmp = -tmp;

  ltime->second_part = (tmp % (1LL << 24));
  ymdhms = (tmp >> 24);

  ymd = ymdhms >> 17;
  ym = ymd >> 5;
  hms = ymdhms % (1 << 17);

  ltime->day = ymd % (1 << 5);
  ltime->month = ym % 13;
  ltime->year = static_cast<uint32_t>(ym / 13);

  ltime->second = hms % (1 << 6);
  ltime->minute = (hms >> 6) % (1 << 6);
  ltime->hour = static_cast<uint32_t>(hms >> 12);

  ltime->time_type = MYSQL_TIMESTAMP_DATETIME;
  ltime->time_zone_displacement = 0;
}

uint32_t g_batch_size = 10;
std::atomic<bool> g_purge_thread_exit = false;
void SetPurgeThreadExit(bool exit) {
  g_purge_thread_exit = exit;
}
bool IsPurgeThreadExit() {
  return (IsExit() || g_purge_thread_exit);
}

std::atomic<bool> g_purge_thread_asks_for_retry = false;

bool IsNodeAlive(unsigned char* last_active);
bool GetShard(Ndb* myNdb, int32_t* shard, int32_t* n_purge_nodes,
              bool update_objects) {
  /*
   * shard:
   * -2: The current node is not a purging node
   * -1: No configure table found, purge by default
   * [0 - X]: shard no
   */
  *shard = -1;
  *n_purge_nodes = 0;
  if (myNdb->setDatabaseName("mysql") != 0) {
    std::cerr << "Failed to select database: " << "mysql"
              << ", error: " << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    return false;
  }
  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  if (update_objects) {
    myDict->removeCachedTable("ttl_purge_nodes");
  }
  const NdbDictionary::Table* tab = myDict->getTable("ttl_purge_nodes");
  if (tab == nullptr) {
    if (myDict->getNdbError().code == 723) {
      // std::cerr << "ttl_purge_nodes table is not found, no sharding"
      //           << std::endl;
      return true;
    } else {
      std::cerr << "Failed to get table: " << "ttl_purge_nodes"
                << ", error: " << myDict->getNdbError().code << "("
                << myDict->getNdbError().message << "), retry..."
                << std::endl;
      return false;
    }
  }
  NdbRecAttr *myRecAttr[3];
  NdbTransaction* trans = nullptr;
  NdbScanOperation *scan_op = nullptr;
  int32_t n_nodes = 0;;
  std::vector<int32_t> purge_nodes;
  size_t pos = 0;
  bool check = 0;
  
  trans = myNdb->startTransaction();
  if (trans == nullptr) {
    std::cerr << "Failed to start transaction, error: "
              << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  scan_op = trans->getNdbScanOperation(tab);
  if (scan_op == nullptr) {
    std::cerr << "Failed to get scan operation, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  if (scan_op->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    std::cerr << "Failed to readTuples, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  myRecAttr[0] = scan_op->getValue("node_id");
  if (myRecAttr[0] == nullptr) {
    std::cerr << "Failed to getValue, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  myRecAttr[1] = scan_op->getValue("last_active");
  if (myRecAttr[1] == nullptr) {
    std::cerr << "Failed to getValue, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    std::cerr << "Failed to execute, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  n_nodes = 0;
  purge_nodes.clear();
  pos = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      if (myRecAttr[0]->int32_value() != myNdb->getNodeId() &&
          (myRecAttr[1]->isNULL() ||
           !IsNodeAlive(reinterpret_cast<unsigned char*>(
              myRecAttr[1]->aRef())))) {
        std::cerr << "Detected inactive purging node "
                  << myRecAttr[0]->int32_value() << ", skip it"
                  << std::endl;
        continue;
      }
      n_nodes++;
      purge_nodes.push_back(myRecAttr[0]->int32_value());
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  std::sort(purge_nodes.begin(), purge_nodes.end());
  if (!purge_nodes.empty()) {
    fprintf(stderr, "Alive configured purge nodes: ");
    for (auto iter : purge_nodes) {
      fprintf(stderr, "%u ", iter);
      if (myNdb->getNodeId() == iter) {
        *shard = pos;
      }
      pos++;
    }
    fprintf(stderr, "\n");
  }
  if (!purge_nodes.empty() && *shard == -1) {
    // if the current node id is not in the purge nodes list,
    // set shard to -2 to tell the purge thread sleep
    *shard = -2;
  }
  *n_purge_nodes = n_nodes;
  myNdb->closeTransaction(trans);
  return true;

err:
  if (trans != nullptr) {
    myNdb->closeTransaction(trans);
  }
  return false;
}

#define DATETIMEF_INT_OFS 0x8000000000LL
int64_t GetNow(unsigned char* now_char) {
  assert(now_char != nullptr);
  int64_t now = 0;
  memset(now_char, 0, 8);
  MYSQL_TIME curr_dt;
  struct tm tmp_tm;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  gmtime_r(&t_now, &tmp_tm);
  curr_dt.neg = false;
  curr_dt.second_part = 0;
  curr_dt.year = ((tmp_tm.tm_year + 1900) % 10000);
  curr_dt.month = tmp_tm.tm_mon + 1;
  curr_dt.day = tmp_tm.tm_mday;
  curr_dt.hour = tmp_tm.tm_hour;
  curr_dt.minute = tmp_tm.tm_min;
  curr_dt.second = tmp_tm.tm_sec;
  curr_dt.time_zone_displacement = 0;
  curr_dt.time_type = MYSQL_TIMESTAMP_DATETIME;
  if (curr_dt.second == 60 || curr_dt.second == 61) {
    curr_dt.second = 59;
  }
  now = Datetime2ll(curr_dt);
  mi_int5store(now_char, (now >> 24) + DATETIMEF_INT_OFS);
  return now;
}

long calc_daynr(uint year, uint month, uint day) {
  long delsum;
  int temp;
  int y = year; /* may be < 0 temporarily */

  if (y == 0 && month == 0) return 0; /* Skip errors */
  /* Cast to int to be able to handle month == 0 */
  delsum = static_cast<long>(365 * y + 31 * (static_cast<int>(month) - 1) +
                             static_cast<int>(day));
  if (month <= 2)
    y--;
  else
    delsum -= static_cast<long>(static_cast<int>(month) * 4 + 23) / 10;
  temp = ((y / 100 + 1) * 3) / 4;
  assert(delsum + static_cast<int>(y) / 4 - temp >= 0);
  return (delsum + static_cast<int>(y) / 4 - temp);
} /* calc_daynr */

uint calc_days_in_year(uint year) {
  return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? 366
                                                                       : 365);
}

const uchar days_in_month[] = {31, 28, 31, 30, 31, 30, 31,
                               31, 30, 31, 30, 31, 0};
void get_date_from_daynr(int64_t daynr, uint *ret_year, uint *ret_month,
                         uint *ret_day) {
  uint year;
  uint temp;
  uint leap_day;
  uint day_of_year;
  uint days_in_year;
  const uchar *month_pos;

  if (daynr <= 365L || daynr >= 3652500) { /* Fix if wrong daynr */
    *ret_year = *ret_month = *ret_day = 0;
  } else {
    year = static_cast<uint>(daynr * 100 / 36525L);
    temp = (((year - 1) / 100 + 1) * 3) / 4;
    day_of_year = static_cast<uint>(daynr - static_cast<long>(year) * 365L) -
                  (year - 1) / 4 + temp;
    while (day_of_year > (days_in_year = calc_days_in_year(year))) {
      day_of_year -= days_in_year;
      (year)++;
    }
    leap_day = 0;
    if (days_in_year == 366) {
      if (day_of_year > 31 + 28) {
        day_of_year--;
        if (day_of_year == 31 + 28)
          leap_day = 1; /* Handle leap year's leap day */
      }
    }
    *ret_month = 1;
    for (month_pos = days_in_month; day_of_year > static_cast<uint>(*month_pos);
         day_of_year -= *(month_pos++), (*ret_month)++)
      ;
    *ret_year = year;
    *ret_day = day_of_year + leap_day;
  }
}

ulonglong TIME_to_ulonglong_datetime(const MYSQL_TIME &my_time) {
  return (static_cast<ulonglong>(my_time.year * 10000UL +
                                 my_time.month * 100UL + my_time.day) *
              1000000ULL +
          static_cast<ulonglong>(my_time.hour * 10000UL +
                                 my_time.minute * 100UL + my_time.second));
}

#define LEASE_SECONDS 10
bool IsNodeAlive(unsigned char* last_active) {
  assert(last_active != nullptr);
  const int64_t intpart = mi_uint5korr(last_active) - DATETIMEF_INT_OFS;
  int64_t last_active_ll = (static_cast<uint64_t>(intpart) << 24);
  MYSQL_TIME ltime;
  LL2datetime(&ltime, last_active_ll);

  longlong sec, days, daynr;
  sec =
      ((ltime.day - 1) * 3600LL * 24LL + ltime.hour * 3600LL +
       ltime.minute * 60LL + ltime.second +
       LEASE_SECONDS);
  days = sec / (3600 * 24LL);
  sec -= days * 3600 * 24LL;
  if (sec < 0) {
    days--;
    sec += 3600 * 24LL;
  }
  ltime.second_part = 0;
  ltime.second = static_cast<uint>(sec % 60);
  ltime.minute = static_cast<uint>(sec / 60 % 60);
  ltime.hour = static_cast<uint>(sec / 3600);
  daynr = calc_daynr(ltime.year, ltime.month, 1) + days;
  get_date_from_daynr(daynr, &ltime.year, &ltime.month, &ltime.day);

  MYSQL_TIME curr_dt;
  struct tm tmp_tm;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  gmtime_r(&t_now, &tmp_tm);
  curr_dt.neg = false;
  curr_dt.second_part = 0;
  curr_dt.year = ((tmp_tm.tm_year + 1900) % 10000);
  curr_dt.month = tmp_tm.tm_mon + 1;
  curr_dt.day = tmp_tm.tm_mday;
  curr_dt.hour = tmp_tm.tm_hour;
  curr_dt.minute = tmp_tm.tm_min;
  curr_dt.second = tmp_tm.tm_sec;
  curr_dt.time_zone_displacement = 0;
  curr_dt.time_type = MYSQL_TIMESTAMP_DATETIME;
  if (curr_dt.second == 60 || curr_dt.second == 61) {
    curr_dt.second = 59;
  }
  uint64_t last_active_add_lease_llu = TIME_to_ulonglong_datetime(ltime);
  uint64_t now_llu = TIME_to_ulonglong_datetime(curr_dt);
  // fprintf(stderr, "lease_expires_at: %lu, now_llu: %lu\n",
  //         last_active_add_lease_llu, now_llu);
  if (last_active_add_lease_llu >= now_llu) {
    return true;
  } else {
    return false;
  }
}

bool UpdateLease(Ndb* myNdb, unsigned char* now_char) {
  if (myNdb->setDatabaseName("mysql") != 0) {
    std::cerr << "Failed to select database: " << "mysql"
              << ", error: " << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    return false;
  }
  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Table* tab = myDict->getTable("ttl_purge_nodes");
  if (tab == nullptr) {
    if (myDict->getNdbError().code == 723) {
      std::cerr << "ttl_purge_nodes table is not found, no lease"
                << std::endl;
      return true;
    } else {
      std::cerr << "Failed to get table: " << "ttl_purge_nodes"
                << ", error: " << myDict->getNdbError().code << "("
                << myDict->getNdbError().message << "), retry..."
                << std::endl;
      return false;
    }
  }
  NdbTransaction* trans = nullptr;
  NdbOperation *op = nullptr;
  
  trans = myNdb->startTransaction();
  if (trans == nullptr) {
    std::cerr << "Failed to start transaction, error: "
              << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  op = trans->getNdbOperation(tab);
  if (op == nullptr) {
    std::cerr << "Failed to get operation, " << "ttl_purge_nodes"
              << ", error: " << trans->getNdbError().code << "("
              << trans->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }
  op->updateTuple();
  op->equal("node_id", myNdb->getNodeId());
  op->setValue("last_active", reinterpret_cast<char*>(now_char));

  if (trans->execute(NdbTransaction::Commit) != 0) {
    if (trans->getNdbError().code != 626 /*not found*/) {
      std::cerr << "Failed to commit, " << "ttl_purge_nodes"
                << ", error: " << trans->getNdbError().code << "("
                << trans->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    }
  }
  myNdb->closeTransaction(trans);
  return true;
err:
  if (trans != nullptr) {
    myNdb->closeTransaction(trans);
  }
  return false;
}

void PurgeTTL(Ndb_cluster_connection* cluster_connection) {
  SetPurgeThreadExit(false);
  Ndb* myNdb = new Ndb(cluster_connection);
  myNdb->setNdbObjectName("TTL record-cleaner");
  NdbDictionary::Dictionary* myDict = nullptr;
  std::map<std::string, TTLInfo> local_ttl_cache;
  const NdbDictionary::Table* ttl_tab = nullptr;
  const NdbDictionary::Index *ttl_index = nullptr; 
  NdbTransaction* trans = nullptr;
  NdbScanOperation *scan_op = nullptr;
  NdbRecAttr *myRecAttr[3];
  myDict = myNdb->getDictionary();
  g_cache_updated = true;
  int32_t shard = -1;
  int32_t n_purge_nodes = 0;
  size_t pos = 0;
  std::string db_str;
  std::string table_str;
  uint32_t ttl_col_no = 0;
  int check = 0;
  NdbError err;
  uint32_t deletedRows = 0;
  int64_t now = 0;
  unsigned char now_char[8];
  unsigned char buf[8];
  int trx_failure_times = 0;
  bool update_objects = false;
  std::map<std::string, TTLInfo>::iterator iter;
  bool purge_trx_started = false;

  if (myNdb->init() != 0) {
    std::cerr << "Failed to init Ndb object, error: "
              << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }

  do {
    purge_trx_started = false;

    update_objects = false;
    if (g_cache_updated) {
      std::cerr << "found global ttl cache updated" << std::endl;
      const std::lock_guard<std::mutex> lock(g_mutex);
      local_ttl_cache = g_ttl_cache;
      g_cache_updated = false;
      std::cerr << "update local ttl cache, total ttl table nums: "
                << local_ttl_cache.size()
                << std::endl;
      update_objects = true;
    }

    shard = -1;
    n_purge_nodes = 0;
    if (GetShard(myNdb, &shard, &n_purge_nodes, update_objects) == false) {
      std::cerr << "Failed to select shard"
                << ", error: " << myNdb->getNdbError().code << "("
                << myNdb->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    } else {
      fprintf(stderr, "Get shard: %d of %d\n", shard, n_purge_nodes);
    }
    if (shard == -2) {
      std::cerr << "I'm not responsible for purging..." << std::endl;
      sleep(2);
      continue;
    }

    {
      GetNow(now_char);
      // update lease if shard is activated
      if (shard >= 0 && !UpdateLease(myNdb, now_char)) {
        std::cerr << "Failed to update the lease" << std::endl;
        goto err;
      }
    }
    if (local_ttl_cache.empty()) {
      std::cerr << "No ttl table is found, no need to purge" << std::endl;
      sleep(1);
      continue;
    }
    for (iter = local_ttl_cache.begin(); iter != local_ttl_cache.end();
         iter++) {
      purge_trx_started = false;
      {
        GetNow(now_char);
        if (shard >= 0 && !UpdateLease(myNdb, now_char)) {
          std::cerr << "Failed to update the lease[2]" << std::endl;
          goto err;
        }
      }
      if (g_cache_updated) {
        break;
      }

      std::cerr << "Processing " << iter->first << ":" << std::endl;

      pos = iter->first.find('/');
      assert(pos != std::string::npos);
      db_str = iter->first.substr(0, pos);
      assert(pos + 1 < iter->first.length());
      table_str = iter->first.substr(pos + 1);
      ttl_col_no = iter->second.col_no;
      check = 0;
      deletedRows = 0;
      now = 0;
      trx_failure_times = 0;

      if (myNdb->setDatabaseName(db_str.c_str()) != 0) {
        std::cerr << "Failed to select database: " << db_str
                  << ", error: " << myNdb->getNdbError().code << "("
                  << myNdb->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      if (update_objects) {
        /*
         * Notice:
         * Based on the comment below,
         * here we need to call invalidateIndex() for ttl_index, the reason is
         * removeCachedTable() just decrease the reference count of the table
         * object in the global list, it won't remove the object even the counter
         * becomes to 0. But invalidateIndex() will set the object to DROP and
         * remove it if the counter is 0. Since we don't call invalidateIndex
         * in main thread(it's a major different with other normal table objects),
         * so here we need to call invalidateIndex()
         */
        myDict->invalidateIndex("ttl_index", table_str.c_str());
        /*
         * Notice:
         * Purge thread can only call removeCachedXXX to remove its
         * thread local cached table object and decrease the reference
         * count of the global cached table object.
         * If we call invalidateTable() and following by getTable() here,
         * Purge thread will invalidate the global cached table object
         * and generate a new version of table object, which will make
         * the main thread's following invalidateTable() + getTable() gets
         * this table object, stops the chance to get the latest one from
         * data nodes.
         */
        myDict->removeCachedTable(table_str.c_str());
      }
      ttl_tab= myDict->getTable(table_str.c_str());
      if (ttl_tab == nullptr) {
        std::cerr << "Failed to get table: " << table_str
                  << ", error: " << myDict->getNdbError().code << "("
                  << myDict->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      if (shard >= 0 && n_purge_nodes > 0 &&
          std::hash<std::string>{}(
            (std::to_string(ttl_tab->getTableId()) + table_str)) %
                           n_purge_nodes != static_cast<uint32_t>(shard)) {
        std::cerr << "    Skip" << std::endl;
        continue;
      }
      std::cerr << "    [P" << iter->second.part_id
                << "/" << ttl_tab->getPartitionCount() << "]" << std::endl;
      assert(iter->second.part_id < ttl_tab->getPartitionCount());

      trx_failure_times = 0;
retry_trx:
      trans = myNdb->startTransaction();
      if (trans == nullptr) {
        std::cerr << "Failed to start transaction, " << table_str
                  << ", error: " << myNdb->getNdbError().code << "("
                  << myNdb->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      purge_trx_started = true;
      
      ttl_index = myDict->getIndex("ttl_index", table_str.c_str());

      check = 0;
      deletedRows = 0;
      now = 0;
      if (ttl_index != nullptr) {
        // Found index on ttl column, use it
        std::cerr << "    Using index scan, ";
        const NdbDictionary::Column* ttl_col_index = ttl_index->getColumn(0);
        assert(ttl_col_index != nullptr && ttl_col_index->getType() ==
               NdbDictionary::Column::Datetime2);
        const NdbDictionary::Column* ttl_col_table =
               ttl_tab->getColumn(ttl_col_index->getName());
        assert(ttl_col_table != nullptr && ttl_col_table->getType() ==
               NdbDictionary::Column::Datetime2 &&
               ttl_col_table->getColumnNo() == static_cast<int>(ttl_col_no));

        NdbIndexScanOperation *index_scan_op =
          trans->getNdbIndexScanOperation(ttl_index);
        index_scan_op->setPartitionId(iter->second.part_id);
        /* Index Scan */
        Uint32 scanFlags=
         /*NdbScanOperation::SF_OrderBy |
          *NdbScanOperation::SF_MultiRange |
          */
          NdbScanOperation::SF_KeyInfo |
          NdbScanOperation::SF_OnlyExpiredScan;

        if (index_scan_op->readTuples(NdbOperation::LM_Exclusive,
              scanFlags,
              1,           // parallel
              g_batch_size // batch
              ) != 0) {
          std::cerr << "Failed to readTuples, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        {
          now = GetNow(now_char);
          std::cerr << "bound [";
          for (Uint32 i = 0; i < 8; i++) {
            std::cerr << std::hex
              << static_cast<unsigned int>(iter->second.last_purged[i])
              << " ";
          }
          std::cerr << " --- ";
          for (Uint32 i = 0; i < 8; i++) {
            std::cerr << std::hex
              << static_cast<unsigned int>(now_char[i])
              << " ";
          }
          std::cerr << "), " << std::endl;
        }
        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundLE,
                            iter->second.last_purged)) {
          std::cerr << "Failed to setBound, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundGT, now_char)) {
          std::cerr << "Failed to setBound, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        myRecAttr[0] = index_scan_op->getValue(ttl_col_no);
        if (myRecAttr[0] == nullptr) {
          std::cerr << "Failed to getValue, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          std::cerr << "Failed to execute, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        memset(buf, 0, 8);
        while ((check = index_scan_op->nextResult(true)) == 0) {
          do {
            memset(buf, 0, 8);
            memcpy(buf, myRecAttr[0]->aRef(),
                   myRecAttr[0]->get_size_in_bytes());
            // std::cerr << "Get a expired row: timestamp = ["
            //   << myRecAttr[0]->get_size_in_bytes() << "]";
            // for (Uint32 i = 0; i < myRecAttr[0]->get_size_in_bytes(); i++) {
            //   std::cerr << std::hex
            //     << static_cast<unsigned int>(myRecAttr[0]->aRef()[i])
            //     << " ";
            // }
            // std::cerr << std::endl;
            if (index_scan_op->deleteCurrentTuple() != 0) {
              std::cerr << "Failed to delete, " << table_str
                        << ", error: " << trans->getNdbError().code << "("
                        << trans->getNdbError().message << "), retry..."
                        << std::endl;
              goto err;
            }
            deletedRows++;
          } while ((check = index_scan_op->nextResult(false)) == 0);

          if (check == -1) {
            std::cerr << "Failed to execute[2], " << table_str
                      << ", error: " << trans->getNdbError().code << "("
                      << trans->getNdbError().message << "), retry..."
                      << std::endl;
            goto err;
          }
          break;
        }
        /**
         * Commit all prepared operations
         */
        if (trans->execute(NdbTransaction::Commit) == -1) {
          std::cerr << "Failed to commit, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        } else if (*(uint64*)(buf) != 0) {
          memcpy(iter->second.last_purged,
                 buf, 8);
        }
      } else if (myDict->getNdbError().code == 4243) {
        // Can't find the index on ttl column, use table instead
        std::cerr << "    Using table scan" << std::endl;
        scan_op = trans->getNdbScanOperation(ttl_tab);
        if (scan_op == nullptr) {
          std::cerr << "Failed to get scan operation, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        scan_op->setPartitionId(iter->second.part_id);
        Uint32 scanFlags= NdbScanOperation::SF_OnlyExpiredScan;
        if (scan_op->readTuples(NdbOperation::LM_Exclusive, scanFlags,
                                1, g_batch_size) != 0) {
          std::cerr << "Failed to readTuples, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        myRecAttr[0] = scan_op->getValue(ttl_col_no);
        if (myRecAttr[0] == nullptr) {
          std::cerr << "Failed to getValue, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          std::cerr << "Failed to execute, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
        while ((check = scan_op->nextResult(true)) == 0) {
          do {
            // std::cerr << "Get a expired row: timestamp = ["
            //   << myRecAttr[0]->get_size_in_bytes() << "]";
            // for (Uint32 i = 0; i < myRecAttr[0]->get_size_in_bytes(); i++) {
            //   std::cerr << std::hex
            //     << static_cast<unsigned int>(myRecAttr[0]->aRef()[i])
            //     << " ";
            // }
            // std::cerr << std::endl;
            if (scan_op->deleteCurrentTuple() != 0) {
              std::cerr << "Failed to delete, " << table_str
                        << ", error: " << trans->getNdbError().code << "("
                        << trans->getNdbError().message << "), retry..."
                        << std::endl;
              goto err;
            }
            deletedRows++;

          } while ((check = scan_op->nextResult(false)) == 0);

          if (check == -1) {
            std::cerr << "Failed to execute[2], " << table_str
                      << ", error: " << trans->getNdbError().code << "("
                      << trans->getNdbError().message << "), retry..."
                      << std::endl;
            goto err;
          }

          break;
        }
        /**
         * Commit all prepared operations
         */
        if (trans->execute(NdbTransaction::Commit) == -1) {
          std::cerr << "Failed to commit, " << table_str
                    << ", error: " << trans->getNdbError().code << "("
                    << trans->getNdbError().message << "), retry..."
                    << std::endl;
          goto err;
        }
      } else {
        std::cerr << "Failed to get Table/Index object, error: "
                  << myDict->getNdbError().code << "("
                  << myDict->getNdbError().message << ")"
                  << std::endl;
        goto err;
      }
      myNdb->closeTransaction(trans);
      trans = nullptr;
      fprintf(stderr, "    Purged %u rows\n", deletedRows);
      iter->second.part_id =
        ((iter->second.part_id + 1) % ttl_tab->getPartitionCount());
      // Finish 1 batch
      // keep the ttl_tab in local table cache ?
      continue;
err:
      if (trans != nullptr) {
        myNdb->closeTransaction(trans);
      }
      trx_failure_times++;
      sleep(1);
      if (trx_failure_times > 10) {
        std::cerr << "PurgeTTL has retried for 10 times...It's time to ask for "
                     "retring from connecting" << std::endl;
        g_purge_thread_asks_for_retry = true;
        SetPurgeThreadExit(true);
        break;
      } else if (purge_trx_started) {
        goto retry_trx;
      } else {
        // retry from begining
        break; // jump out from for-loop
      }
    }
    // Finish 1 round
    sleep(2);
  } while (!IsPurgeThreadExit());
  
  delete myNdb;
  std::cerr << "PurgeTTL thread exit" << std::endl;
  return;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cout << "Arguments are <connect_string cluster>\n";
    exit(-1);
  }
  const char *connectstring = argv[1];

  const char* eventName = "REPL$mysql/ndb_schema";
  const char* schema_db_name = "mysql";
  const char* schema_tab_name = "ndb_schema";
  const char* schema_res_tab_name = "ndb_schema_result";
  NdbDictionary::Dictionary* myDict = nullptr;
  const NdbDictionary::Table* schema_tab = nullptr;
  const NdbDictionary::Table* schema_res_tab = nullptr;
  const int noEventColumnName = 10;
  const char *eventColumnName[noEventColumnName] = {
    "db",
    "name",
    "slock",
    "query",
    "node_id",
    "epoch",
    "id",
    "version",
    "type",
    "schema_op_id"
  };
  typedef union {
    NdbRecAttr* ra;
    NdbBlob* bh;
  } RA_BH;
  char slock_buf_pre[32];
  char slock_buf[32];
  bool initialized_cache = false;
  const char* message_buf = "API_OK";
  char event_name_buf[128];
  uint32_t event_nums = 0;
  bool init_succ = false;
  NdbEventOperation *ev_op = nullptr;
  NdbEventOperation *op = nullptr;
  std::thread purge_thread;

  ndb_init();

retry_from_connecting:
  Ndb_cluster_connection *cluster_connection = new Ndb_cluster_connection(
      connectstring);

  int r = cluster_connection->connect(-1 /* retries               */,
                                      3 /* delay between retries */,
                                      1 /* verbose               */);
  if (r > 0) {
    std::cout
        << "Cluster connect failed, possibly resolved with more retries.\n";
    exit(-1);
  } else if (r < 0) {
    std::cout << "Cluster connect failed.\n";
    exit(-1);
  }

  if (cluster_connection->wait_until_ready(30, 30)) {
    std::cout << "Cluster was not ready within 30 secs." << std::endl;
    exit(-1);
  }

  Ndb *myNdb = new Ndb(cluster_connection);
  myNdb->setNdbObjectName("TTL schema-watcher");

  if (myNdb->init() != 0) {
    std::cerr << "Failed to init Ndb object, error: " 
              << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry..."
              << std::endl;
    goto err;
  }

  myDict = nullptr;
  schema_tab = nullptr;
  schema_res_tab = nullptr;
  initialized_cache = false;
  init_succ = false;
  g_ttl_cache.clear();
  ev_op = nullptr;
  op = nullptr;

  do {

    if (myNdb->setDatabaseName(schema_db_name) != 0) {
      std::cerr << "Failed to select system database: " << schema_db_name 
                << ", error: " << myNdb->getNdbError().code << "("
                << myNdb->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    }
    myDict = myNdb->getDictionary();
    schema_tab= myDict->getTable(schema_tab_name);
    if (schema_tab == nullptr) {
      std::cerr << "Failed to get system table: " << schema_tab_name
                << ", error: " << myNdb->getNdbError().code << "("
                << myNdb->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    }
    schema_res_tab= myDict->getTable(schema_res_tab_name);
    if (schema_res_tab == nullptr) {
      std::cerr << "Failed to get system table: " << schema_res_tab_name
                << ", error: " << myNdb->getNdbError().code << "("
                << myNdb->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    }

    NdbDictionary::Event my_event(eventName);
    my_event.setTable(*schema_tab);
    my_event.addTableEvent(NdbDictionary::Event::TE_ALL);
    my_event.mergeEvents(true);
    my_event.setReportOptions(NdbDictionary::Event::ER_ALL | NdbDictionary::Event::ER_SUBSCRIBE |
                              NdbDictionary::Event::ER_DDL);
    const int n_cols = schema_tab->getNoOfColumns();
    for (int i = 0; i < n_cols; i++) {
      // const NdbDictionary::Column* col = schema_tab->getColumn(i);
      // std::cerr << "Column: " << col->getName()
      //           << ", type: " << col->getType()
      //           << ", length: " << col->getLength()
      //           << ", size_in_bytes: " << col->getSizeInBytes() << std::endl;
      my_event.addEventColumn(i);
    }

    NdbDictionary::Dictionary *dict = myNdb->getDictionary();
    if (dict->createEvent(my_event)) {
      if (dict->getNdbError().classification != NdbError::SchemaObjectExists) {
        std::cerr << "Failed to create event, error: "
                  << dict->getNdbError().code << "("
                  << dict->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
    }
    NdbDictionary::Event_ptr ev(dict->getEvent(eventName));
    if (ev) {
      // The event already exists in NDB
      init_succ = true;
    } else {
      if (dict->getNdbError().code == NDB_INVALID_SCHEMA_OBJECT &&
          dict->dropEvent(my_event.getName(), 1)) {
        std::cerr << "Failed to drop the old event, error: "
                  << dict->getNdbError().code << "("
                  << dict->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      std::cerr << "Failed to get the event, error: "
                << dict->getNdbError().code << "("
                << dict->getNdbError().message << "), "
                << "drop the old one and retry..."
                << std::endl;
    }
  } while (!init_succ);

  if ((ev_op = myNdb->createEventOperation(eventName)) == nullptr) {
    std::cerr << "Failed to create event operation, error: "
              << myNdb->getNdbError().code << "("
              << myNdb->getNdbError().message << "), retry... "
              << std::endl;
    goto err;
  }
  ev_op->mergeEvents(true);

  RA_BH recAttr[noEventColumnName];
  RA_BH recAttrPre[noEventColumnName];
	for (int i = 0; i < noEventColumnName; i++) {
    if (i != 3) {
      recAttr[i].ra = ev_op->getValue(eventColumnName[i]);
      recAttrPre[i].ra = ev_op->getPreValue(eventColumnName[i]);
    } else {
      recAttr[i].bh = ev_op->getBlobHandle(eventColumnName[i]);
      recAttrPre[i].bh = ev_op->getPreBlobHandle(eventColumnName[i]);
    }
	}

  if (ev_op->execute()) {
    std::cerr << "Failed to execute event operation, error: "
              << ev_op->getNdbError().code << "("
              << ev_op->getNdbError().message << "), retry... "
              << std::endl;
    goto err;
  }

  if (initialized_cache == false) {
    g_ttl_cache.clear();
    NdbDictionary::Dictionary::List list;
    if (myDict->listObjects(list, NdbDictionary::Object::UserTable) != 0) {
      std::cerr << "Failed to list objects, error: "
                << myDict->getNdbError().code << "("
                << myDict->getNdbError().message << "), retry... "
                << std::endl;
      goto err;
    }

    for (uint i = 0; i < list.count; i++) {
      NdbDictionary::Dictionary::List::Element &elmt = list.elements[i];

      const char* db_str = elmt.database;
      assert(elmt.schema == std::string("def"));  // always "<db>/def/<name>"
      const char *table_str = elmt.name;
      // std::cerr << "Hi " << db_str << "/" << table_str << std::endl;
      if (strcmp(db_str, "mysql") == 0) {
        continue;
      }

      if (myNdb->setDatabaseName(db_str) != 0) {
        std::cerr << "Failed to select database: " << db_str
                  << ", error: " << myNdb->getNdbError().code << "("
                  << myNdb->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      const NdbDictionary::Table* tab= myDict->getTable(
          table_str);
      if (tab == nullptr) {
        std::cerr << "Failed to get table: " << table_str
                  << ", error: " << myDict->getNdbError().code << "("
                  << myDict->getNdbError().message << "), retry..."
                  << std::endl;
        goto err;
      }
      UpdateLocalCache(db_str, table_str, tab);
    }
    initialized_cache = true;
  }

  purge_thread = std::thread(PurgeTTL, cluster_connection);

  while (!IsExit()) {
    int res = myNdb->pollEvents(1000);  // wait for event or 1000 ms
    if (res > 0) {
      while ((op = myNdb->nextEvent())) {
        if (op->hasError()) {
          std::cerr << "Get an event error, " << op->getNdbError().code
                    << "(" << op->getNdbError().message
                    << ") on handling ndb_schema event, retry..."
                    << std::endl;
          goto err;
        }
        event_nums++;
        std::cerr << "EVENT [" << event_nums << "]: "
          << GetEventName(op->getEventType(), event_name_buf)
          << ", GCI = " << op->getGCI() << std::endl;
        char* ptr = nullptr;
        char* ptr_pre = nullptr;
        std::string db_str_pre;
        std::string db_str;
        std::string table_str_pre;
        std::string table_str;
        std::string query_str_pre;
        std::string query_str;
        uint32_t node_id = 0;
        uint32_t type = 0;
        uint32_t id = 0;
        uint32_t schema_op_id = 0;
        NdbTransaction* trans = nullptr;
        NdbOperation* top = nullptr;
        bool clear_slock = false;
        bool trx_succ = false;
        uint32_t trx_failure_times = 0;
        bool cache_updated = false;
        std::cerr << "----------------------------" << std::endl;
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
            for (int l = 0; l < noEventColumnName; l++) {
              ptr_pre = recAttrPre[l].ra->aRef();
              ptr = recAttr[l].ra->aRef();
              switch(l) {
                case 0:
                  db_str_pre = std::string(ptr_pre + 1,
                      recAttrPre[l].ra->u_8_value());
                  db_str = std::string(ptr + 1,
                      recAttr[l].ra->u_8_value());
                  std::cerr << "  db: "
                    << "[" << static_cast<unsigned int>(
                               recAttrPre[l].ra->u_8_value())
                    << "]"
                    << db_str_pre << " -> "
                    << "[" << static_cast<unsigned int>(
                               recAttr[l].ra->u_8_value())
                    << "]"
                    << db_str << std::endl;
                  break;
                case 1:
                  table_str_pre = std::string(ptr_pre + 1,
                      recAttrPre[l].ra->u_8_value());
                  table_str = std::string(ptr + 1,
                      recAttr[l].ra->u_8_value());
                  std::cerr << "  table: "
                    << "[" << static_cast<unsigned int>(
                               recAttrPre[l].ra->u_8_value())
                    << "]"
                    << table_str_pre << " -> "
                    << "[" << static_cast<unsigned int>(
                               recAttr[l].ra->u_8_value())
                    << "]"
                    << table_str << std::endl;
                  break;
                case 2:
                  memset(slock_buf_pre, 0, 32);
                  memcpy(slock_buf_pre, recAttrPre[l].ra->aRef(), 32);
                  std::cerr << "  slock: ";
                  for (int i = 0; i < 32; i++) {
                    std::cerr << static_cast<unsigned int>(slock_buf_pre[i]) << " ";
                  }
                  std::cerr << std::endl;
                  std::cerr << "       ->";
                  memset(slock_buf, 0, 32);
                  memcpy(slock_buf, recAttr[l].ra->aRef(), 32);
                  for (int i = 0; i < 32; i++) {
                    std::cerr << static_cast<unsigned int>(slock_buf[i]) << " ";
                  }
                  std::cerr << std::endl;
                  break;
                case 3:
                  {
                    int blob_is_null = 0;
                    Uint64 blob_len = 0;
                    recAttrPre[l].bh->getNull(blob_is_null);
                    recAttrPre[l].bh->getLength(blob_len);
                    std::cerr << "  query: ";
                    if (blob_is_null == 0 && blob_len != 0) {
                      std::cerr << "[" << blob_len <<"] ";
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str_pre.resize(read_len, '\0');
                      recAttrPre[l].bh->readData(query_str_pre.data(), read_len);
                      std::cerr << query_str_pre;
                    } else {
                      std::cerr << "[0] ";
                    }
                    std::cerr << std::endl;
                    std::cerr << "       ->";
                    blob_is_null = 0;
                    blob_len = 0;
                    recAttr[l].bh->getNull(blob_is_null);
                    recAttr[l].bh->getLength(blob_len);
                    if (blob_is_null == 0 && blob_len != 0) {
                      std::cerr << "[" << blob_len <<"] ";
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str.resize(read_len, '\0');
                      recAttr[l].bh->readData(query_str.data(), read_len);
                      std::cerr << query_str;
                    } else {
                      std::cerr << "[0]";
                    }
                    std::cerr << std::endl;
                    break;
                  }
                case 4:
                  node_id = recAttr[l].ra->u_32_value();
                  std::cerr << "  node_id: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << node_id << std::endl;
                  break;
                case 5:
                  std::cerr << "  epoch: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << recAttr[l].ra->u_32_value() << std::endl;
                  break;
                case 6:
                  id = recAttr[l].ra->u_32_value();
                  std::cerr << "  id: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << recAttr[l].ra->u_32_value() << std::endl;
                  break;
                case 7:
                  std::cerr << "  version: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << recAttr[l].ra->u_32_value() << std::endl;
                  break;
                case 8:
                  // SCHEMA_OP_TYPE
                  type = recAttr[l].ra->u_32_value();
                  std::cerr << "  type: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << type << std::endl;
                  break;
                case 9:
                  schema_op_id = recAttr[l].ra->u_32_value();
                  std::cerr << "  schema_op_id: "
                    << recAttrPre[l].ra->u_32_value() << " -> "
                    << schema_op_id << std::endl;
                  break;
                default:
                  break;
              }
            }
            std::cerr << "----------------------------" << std::endl;

            clear_slock = false;
            cache_updated = false;
            switch(type) {
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
                  if (myNdb->setDatabaseName(db_str.c_str()) != 0) {
                    std::cerr << "Failed to select database: " << db_str
                              << ", error: " << myNdb->getNdbError().code << "("
                              << myNdb->getNdbError().message << "), retry..."
                              << std::endl;
                    goto err;
                  }
                  myDict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab= myDict->getTable(
                      new_table_str.c_str());
                  if (tab == nullptr) {
                    std::cerr << "Failed to get table: " << new_table_str
                              << ", error: " << myDict->getNdbError().code << "("
                              << myDict->getNdbError().message << "), retry..."
                              << std::endl;
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(g_mutex);
                  cache_updated = UpdateLocalCache(db_str, table_str,
                                                    new_table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_TABLE:
                {
                  const std::lock_guard<std::mutex> lock(g_mutex);
                  cache_updated = UpdateLocalCache(db_str, table_str, nullptr);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_DB:
                {
                  const std::lock_guard<std::mutex> lock(g_mutex);
                  cache_updated = DropDBLocalCache(db_str);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CREATE_TABLE:
              case SCHEMA_OP_TYPE::SOT_ALTER_TABLE_COMMIT: 
              case SCHEMA_OP_TYPE::SOT_ONLINE_ALTER_TABLE_COMMIT: 
                {
                  if (myNdb->setDatabaseName(db_str.c_str()) != 0) {
                    std::cerr << "Failed to select database: " << db_str
                              << ", error: " << myNdb->getNdbError().code << "("
                              << myNdb->getNdbError().message << "), retry..."
                              << std::endl;
                    goto err;
                  }
                  myDict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab= myDict->getTable(
                      table_str.c_str());
                  if (tab == nullptr) {
                    std::cerr << "Failed to get table: " << table_str
                              << ", error: " << myDict->getNdbError().code << "("
                              << myDict->getNdbError().message << "), retry..."
                              << std::endl;
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(g_mutex);
                  cache_updated = UpdateLocalCache(db_str, table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CLEAR_SLOCK:
                clear_slock = true;
                break;
              default:
                break;
            }

            // Only cleaner can set g_cache_updated to false;
            if (cache_updated) {
              g_cache_updated = true;
            }

            if (clear_slock) {
              continue;
            }

            trx_succ = false;
            trx_failure_times = 0;
            do {
              trans = myNdb->startTransaction();
              if (trans == nullptr) {
                std::cerr << "Failed to start transaction, error: "
                          << myNdb->getNdbError().code << "("
                          << myNdb->getNdbError().message << "), retry..."
                          << std::endl;
                goto trx_err;
              }
              top = trans->getNdbOperation(schema_res_tab);
              if (top == nullptr) {
                std::cerr << "Failed to get Ndb operation, error: "
                          << trans->getNdbError().code << "("
                          << trans->getNdbError().message << "), retry..."
                          << std::endl;
                goto trx_err;
              }
              if (top->insertTuple() != 0 ||
                  /*Ndb_schema_result_table::COL_NODEID*/
                  top->equal("nodeid", node_id) != 0 ||
                  /*Ndb_schema_result_table::COL_SCHEMA_OP_ID*/
                  top->equal("schema_op_id", schema_op_id) != 0 ||
                  /*Ndb_schema_result_table::COL_PARTICIPANT_NODEID*/
                  top->equal("participant_nodeid", myNdb->getNodeId()) != 0 ||
                  /*Ndb_schema_result_table::COL_RESULT*/
                  top->setValue("result", 0) != 0 ||
                  /*Ndb_schema_result_table::COL_MESSAGE*/
                  top->setValue("message", message_buf) != 0) {
                std::cerr << "Failed to insert tuple, error: "
                          << top->getNdbError().code << "("
                          << top->getNdbError().message << "), retry..."
                          << std::endl;
                goto trx_err;
              }
              if (trans->execute(NdbTransaction::Commit,
                    NdbOperation::DefaultAbortOption,
                    1 /*force send*/) != 0) {
                APIERROR(trans->getNdbError());
                std::cerr << "Failed to execute transaction, error: "
                          << trans->getNdbError().code << "("
                          << trans->getNdbError().message << "), retry..."
                          << std::endl;
                goto trx_err;
              } else {
                trx_succ = true;
              }
trx_err:
              if (trans != nullptr) {
                myNdb->closeTransaction(trans);
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
    } else if (g_purge_thread_asks_for_retry) {
      std::cerr << "Purge thread asks for retry" << std::endl;
      goto err;
    } else if (res < 0) {
      std::cerr << "Failed to poll event, error: "
                << myNdb->getNdbError().code << "("
                << myNdb->getNdbError().message << "), retry..."
                << std::endl;
      goto err;
    }
  }
err:
  if (ev_op != nullptr) {
    myNdb->dropEventOperation(ev_op);
  }
  ev_op = nullptr;
  op = nullptr;
  myDict->dropEvent(eventName);
  if (purge_thread.joinable()) {
    SetPurgeThreadExit(true);
    purge_thread.join();
  }
  delete myNdb;
  delete cluster_connection;
  if (!IsExit()) {
    goto retry_from_connecting;
  }
  ndb_end(0);
  return 0;
}
