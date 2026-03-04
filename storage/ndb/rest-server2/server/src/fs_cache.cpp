/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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

#include "fs_cache.hpp"
#include "config_structs.hpp"
#include "ndb_event_utils.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_dal.hpp"
#include "feature_store/feature_store.h"
#include "rdrs_rondb_connection_pool.hpp"
#include "ndb_api_helper.hpp"
#include "db_operations/pk/common.hpp"

#include <iostream>
#include <memory>
#include <openssl/evp.h>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <NdbThread.h>
#include <NdbApi.hpp>
#include <util/require.h>
#include <util/rondb_hash.hpp>
#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_FS 1
//#define DEBUG_FS_THREAD 1
//#define DEBUG_FS_TIME 1
//#define DEBUG_FS_METADATA 1
#endif

#ifdef DEBUG_FS_METADATA
#define DEB_FS_METADATA(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_FS_METADATA(...) do { } while (0)
#endif

#ifdef DEBUG_FS
#define DEB_FS(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_FS(...) do { } while (0)
#endif

#ifdef DEBUG_FS_THREAD
#define DEB_FS_THREAD(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_FS_THREAD(...) do { } while (0)
#endif

#ifdef DEBUG_FS_TIME
#define DEB_FS_TIME(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_FS_TIME(...) do { } while (0)
#endif

#ifndef DEBUG_FS_THREAD
#define CLEANUP_SLEEP_TIME 10
#else
#define CLEANUP_SLEEP_TIME 1000
#endif
extern EventLogger *g_eventLogger;
extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

FSMetadataCache *g_fs_metadata_cache = nullptr;

void start_fs_cache() {
  g_fs_metadata_cache = new FSMetadataCache();
  require(g_fs_metadata_cache != nullptr);
  DEB_FS("FS Metadata Cache started: %p", g_fs_metadata_cache);
  g_fs_metadata_cache->start_fs_cache_thread();
}

extern "C" void* fs_key_thread_main(void *thr_arg) {
  errno = 0;
  Uint64 key_cache_id = (Uint64)thr_arg;
  Uint32 id = Uint32(key_cache_id);
  g_fs_metadata_cache->cache_entry_updater(id);
  return nullptr;
}

void FSMetadataCache::start_fs_cache_thread() {
  for (Uint64 i = 0; i < NUM_FS_CACHES; i++) {
    m_first_cache_entry[i] = nullptr;
    m_last_cache_entry[i] = nullptr;
    NdbThread *thread = NdbThread_Create(fs_key_thread_main,
                                         (void**)i,
                                          128 * 1024,
                                          "FS Key Cache thread",
                                          NDB_THREAD_PRIO_LOW);
    require(thread != nullptr);
    m_cache_threads[i] = thread;
  }
  return;
}

void stop_fs_cache() {
  DEB_FS("FS Metadata Cache stopped: %p", g_fs_metadata_cache);
  if (g_fs_metadata_cache != nullptr) {
    delete g_fs_metadata_cache;
    g_fs_metadata_cache = nullptr;
  }
}

void fs_cache_dec_ref_count(char *cache_entry) {
  if (cache_entry) {
    FSCacheEntry *cacheEntry = (FSCacheEntry*)cache_entry;
    int prev = cacheEntry->m_ref_count.fetch_sub(1);
    if (prev == 1 && cacheEntry->m_evicted) {
      // Last reference to an evicted entry — safe to delete.
      // No complex features to unregister since m_data is nullptr
      // for entries evicted via evict_failed_entry().
      delete cacheEntry;
    }
  }
}

FSMetadataCache::FSMetadataCache() : m_fs_cache() {
  m_is_thread_running = false;
  m_event_watcher_thread = nullptr;
  m_event_name = "RDRS_FV_EVT_" + generate_event_uuid();
  for (Uint64 i = 0; i < NUM_FS_CACHES; i++) {
    m_rwLock[i] = NdbMutex_Create();
    m_queueLock[i] = NdbMutex_Create();
    m_cache_threads[i] = nullptr;
  }
  DEB_FS("rwLock: %p, queueLock: %p", m_rwLock[0], m_queueLock[0]);
  m_sleepLock = NdbMutex_Create();
  m_sleepCond = NdbCondition_Create();
}

void FSMetadataCache::cleanup() {
  /* Start by waking all threads */
  DEB_FS("Cleanup started");
  NdbMutex_Lock(m_sleepLock);
  for (int i = 0; i < NUM_FS_CACHES; i++)
    NdbMutex_Lock(m_rwLock[i]);
  m_stopped = true;
  NdbCondition_Broadcast(m_sleepCond);
  NdbMutex_Unlock(m_sleepLock);
  for (int i = 0; i < NUM_FS_CACHES; i++)
    NdbMutex_Unlock(m_rwLock[i]);

  /* Wait for all objects to complete cleanup */
  for (int i = 0; i < NUM_FS_CACHES; i++) {
    NdbMutex_Lock(m_rwLock[i]);
    while (m_fs_cache[i].size() > 0) {
#ifdef DEBUG_FS
      Uint32 fs_cache_size = (Uint32)m_fs_cache[i].size();
#endif
      NdbMutex_Unlock(m_rwLock[i]);
      DEB_FS("m_fs_cache[%d].size() = %u", i, fs_cache_size);
      NdbSleep_MilliSleep(CLEANUP_SLEEP_TIME);
      NdbMutex_Lock(m_rwLock[i]);
    }
    NdbMutex_Unlock(m_rwLock[i]);
  }
  while (true) {
    NdbMutex_Lock(m_sleepLock);
    if (m_is_thread_running) {
      NdbMutex_Unlock(m_sleepLock);
    } else {
      NdbMutex_Unlock(m_sleepLock);
      break;
    }
  }
  /* Wait for all threads to finish and destroy their handles */
  for (int i = 0; i < NUM_FS_CACHES; i++) {
    if (m_cache_threads[i] != nullptr) {
      NdbThread_WaitFor(m_cache_threads[i], nullptr);
      NdbThread_Destroy(&m_cache_threads[i]);
      m_cache_threads[i] = nullptr;
    }
  }
  if (m_event_watcher_thread != nullptr) {
    NdbThread_WaitFor(m_event_watcher_thread, nullptr);
    NdbThread_Destroy(&m_event_watcher_thread);
    m_event_watcher_thread = nullptr;
  }
  DEB_FS("Cleanup finished");
}

metadata::FeatureViewMetadata* fs_metadata_cache_get(
  const std::string &fs_key,
  FSCacheEntry** entry) {
  return g_fs_metadata_cache->get_fs_metadata(fs_key, entry);
}

void fs_metadata_update_cache(
  metadata::FeatureViewMetadata *data,
  FSCacheEntry* entry,
  std::shared_ptr<RestErrorCode> errorCode) {
  return g_fs_metadata_cache->update_cache(data, entry, errorCode);
}

void fs_metadata_evict_failed_entry(FSCacheEntry *entry) {
  g_fs_metadata_cache->evict_failed_entry(entry);
}

void FSMetadataCache::evict_failed_entry(FSCacheEntry *entry) {
  Uint32 key_cache_id = entry->m_key_cache_id;
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  auto it = m_fs_cache[key_cache_id].find(entry->m_key);
  if (it == m_fs_cache[key_cache_id].end() || it->second != entry) {
    // Already removed or replaced by another thread
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    return;
  }
  DEB_FS("Evicting failed entry %s from cache", entry->m_key.c_str());
  m_fs_cache[key_cache_id].erase(it);
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  remove_entry(entry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  // Mark as evicted so the last fs_cache_dec_ref_count call deletes it.
  // This must be set after removing from the map — the caller still holds
  // ref_count >= 1, so the entry won't be deleted prematurely.
  entry->m_evicted = true;
}

metadata::FeatureViewMetadata*
FSMetadataCache::get_fs_metadata(const std::string &fs_key,
                                 FSCacheEntry** entry) {
  *entry = nullptr;
#if (NUM_FS_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(fs_key.c_str(), fs_key.size());
#endif
  Uint32 key_cache_id = hash & (NUM_FS_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  if (m_stopped) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    DEB_FS("FS Metadata cache shutdown, Line: %u", __LINE__);
    return nullptr;
  }
  auto it = m_fs_cache[key_cache_id].find(fs_key);

  if (it == m_fs_cache[key_cache_id].end()) {
    *entry = allocate_empty_cache_entry(fs_key, key_cache_id);
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    DEB_FS("FS Key not found, Line: %u", __LINE__);
    return nullptr;
  }
  auto cacheEntry = it->second;
  NdbMutex_Lock(cacheEntry->m_waitLock);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  cacheEntry->m_ref_count++;
  if (cacheEntry->m_state == FSCacheEntry::IS_INVALID) {
#ifdef DEBUG_FS
    int ref_count = cacheEntry->m_ref_count;
#endif
    NdbMutex_Unlock(cacheEntry->m_waitLock);
    DEB_FS("FS Key found invalid, Line: %u, refCount: %d",
             __LINE__, ref_count);
    require(cacheEntry->m_errorCode != nullptr);
    *entry = cacheEntry;
    return nullptr;
  }
  while (cacheEntry->m_state == FSCacheEntry::IS_FILLING) {
    NdbCondition_Wait(cacheEntry->m_waitCond, cacheEntry->m_waitLock);
  }
  if (cacheEntry->m_state == FSCacheEntry::IS_INVALID) {
#ifdef DEBUG_FS
    int ref_count = cacheEntry->m_ref_count;
#endif
    NdbMutex_Unlock(cacheEntry->m_waitLock);
    DEB_FS("FS Key found invalid, Line: %u, refCount: %d",
            __LINE__, ref_count);
    require(cacheEntry->m_errorCode != nullptr);
    *entry = cacheEntry;
    return nullptr;
  }
  require(cacheEntry->m_state == FSCacheEntry::IS_VALID);
#ifdef DEBUG_FS
  {
    int ref_count = cacheEntry->m_ref_count;
    DEB_FS("Key: %s returned, refCount: %d",
           cacheEntry->m_key.c_str(), ref_count);
  }
#endif
  *entry = cacheEntry;
  metadata::FeatureViewMetadata *data = cacheEntry->m_data;
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  cacheEntry->m_lastUsed = NdbTick_getCurrentTicks();
  remove_entry(cacheEntry, key_cache_id);
  insert_last(cacheEntry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  NdbMutex_Unlock(cacheEntry->m_waitLock);
  return data;
}

FSCacheEntry*
FSMetadataCache::allocate_empty_cache_entry(
  const std::string &fs_key,
  const Uint32 key_cache_id) {

  auto newCacheEntry = new FSCacheEntry();
  if (newCacheEntry == nullptr) {
    DEB_FS("FS Key create CacheEntry failed, Line: %u", __LINE__);
    return nullptr;
  }
  DEB_FS("FS Key %s inserted in cache with refCount: 1", fs_key.c_str());
  // Start with ref_count=1 so the eviction thread won't evict this entry
  // while we release the lock and do the slow DB fetch below.  Decremented
  // back to 0 after the entry state is set to IS_VALID/IS_INVALID.
  newCacheEntry->m_ref_count = 1;
  newCacheEntry->m_key_cache_id = key_cache_id;
  newCacheEntry->m_key = fs_key;
  m_fs_cache[key_cache_id][fs_key] = newCacheEntry;

  /* Insert last in double linked list for handling refresh interval */
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  newCacheEntry->m_lastUsed = NdbTick_getCurrentTicks();
  insert_last(newCacheEntry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  return newCacheEntry;
}

void FSMetadataCache::update_cache(
  metadata::FeatureViewMetadata *data,
  FSCacheEntry* entry,
  std::shared_ptr<RestErrorCode> errorCode) {
  /**
   * The cache could be in cleanup state, we only check for cleanup
   * state when retrieving from the cache, most likely a shutdown should
   * not happen while we are retrieving from the database the feature
   * store metadata. Even if it happens the cleanup isn't completed until
   * the use of this cached metadata is completed.
   */

  NdbMutex_Lock(entry->m_waitLock);
  entry->m_data = data;
  entry->m_errorCode = errorCode;
  if (data != nullptr) {
#ifdef DEBUG_FS_METADATA
    DEB_FS_METADATA("Key %s have metadata",
                    entry->m_key.c_str());
#endif
    entry->m_state = FSCacheEntry::IS_VALID;
    DEB_FS("FS Key create CacheEntry succeeded, valid, Line: %u", __LINE__);
  } else {
    DEB_FS("FS Key create CacheEntry succeeded, invalid, Line: %u", __LINE__);
    entry->m_state = FSCacheEntry::IS_INVALID;
  }
#ifdef DEBUG_FS
  {
    int ref_count = entry->m_ref_count;
    DEB_FS("Key: %s set refCount to %d",
           entry->m_key.c_str(), ref_count);
  }
#endif
  NdbCondition_Broadcast(entry->m_waitCond);
  NdbMutex_Unlock(entry->m_waitLock);
  return;
}

void FSMetadataCache::insert_last(FSCacheEntry *entry, Uint32 key_cache_id) {
  entry->m_next_cache_entry = nullptr;
  entry->m_prev_cache_entry = m_last_cache_entry[key_cache_id];
  if (m_first_cache_entry[key_cache_id] == nullptr) {
    m_first_cache_entry[key_cache_id] = entry;
  } else {
    m_last_cache_entry[key_cache_id]->m_next_cache_entry = entry;
  }
  m_last_cache_entry[key_cache_id] = entry;
}

void FSMetadataCache::remove_entry(FSCacheEntry *entry, Uint32 key_cache_id) {
  if (entry == m_first_cache_entry[key_cache_id]) {
    assert(entry->m_prev_cache_entry == nullptr);
    m_first_cache_entry[key_cache_id] = entry->m_next_cache_entry;
    if (entry->m_next_cache_entry != nullptr) {
      entry->m_next_cache_entry->m_prev_cache_entry = nullptr;
    } else {
      m_last_cache_entry[key_cache_id] = nullptr;
    }
  } else if (entry == m_last_cache_entry[key_cache_id]) {
    assert(entry->m_next_cache_entry == nullptr);
    assert(entry->m_prev_cache_entry != nullptr);
    entry->m_prev_cache_entry->m_next_cache_entry = nullptr;
    m_last_cache_entry[key_cache_id] = entry->m_prev_cache_entry;
  } else {
    entry->m_next_cache_entry->m_prev_cache_entry = entry->m_prev_cache_entry;
    entry->m_prev_cache_entry->m_next_cache_entry = entry->m_next_cache_entry;
  }
  entry->m_next_cache_entry = nullptr;
  entry->m_prev_cache_entry = nullptr;
}

void FSMetadataCache::cache_entry_updater(Uint32 key_cache_id) {
  m_is_thread_running = true;
  const Uint64 eviction_ms =
   (Uint64)globalConfigs.featureStore.featureStoreMetadataCache.cacheUnusedEntriesEvictionMS;
  while (true) {
    Uint32 sleepMillis = 100;
    NdbMutex_Lock(m_rwLock[key_cache_id]);
    FSCacheEntry* first_entry = m_first_cache_entry[key_cache_id];
    if (first_entry != nullptr) {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      NdbMutex_Lock(first_entry->m_waitLock);
      if (first_entry->m_ref_count == 0) {
        NDB_TICKS lastUsed = first_entry->m_lastUsed;
        Uint64 milliSeconds = NdbTick_Elapsed(lastUsed, now).milliSec();
        if (m_stopped || (milliSeconds >= eviction_ms)) {
          DEB_FS("FS Key %s deleted", first_entry->m_key.c_str());
          m_fs_cache[key_cache_id].erase(first_entry->m_key);
          NdbMutex_Unlock(m_rwLock[key_cache_id]);
          NdbMutex_Lock(m_queueLock[key_cache_id]);
          remove_entry(first_entry, key_cache_id);
          NdbMutex_Unlock(m_queueLock[key_cache_id]);
          NdbMutex_Unlock(first_entry->m_waitLock);
          delete first_entry;
          continue;
        }
      } else {
#ifdef DEBUG_FS
      int ref_count = first_entry->m_ref_count;
      DEB_FS("FS Key %s ready for delete, ref_count: %d",
             first_entry->m_key.c_str(), ref_count);
#endif
      }
      NdbMutex_Unlock(first_entry->m_waitLock);
    } else if (m_stopped) {
      /* We have no more cache entries to update so can safely stop here */
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      NdbMutex_Lock(m_sleepLock);
      m_is_thread_running = false;
      NdbMutex_Unlock(m_sleepLock);
      DEB_FS("Stop FS cache thread");
      return;
    }
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    NdbMutex_Lock(m_sleepLock);
    if (!m_stopped)
      NdbCondition_WaitTimeout(m_sleepCond,
                               m_sleepLock,
                               sleepMillis);
    NdbMutex_Unlock(m_sleepLock);
  }
}

bool FSMetadataCache::load_single_feature_view(const std::string &fsName,
                                               const std::string &fvName,
                                               int fvVersion) {
  std::string cacheKey =
    metadata::getFeatureViewCacheKey(fsName, fvName, fvVersion);

#if (NUM_FS_CACHES == 1)
  Uint32 key_cache_id = 0;
#else
  Uint32 hash = rondb_xxhash_std(cacheKey.c_str(), cacheKey.size());
  Uint32 key_cache_id = hash & (NUM_FS_CACHES - 1);
#endif

  NdbMutex_Lock(m_rwLock[key_cache_id]);
  if (m_stopped) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    return true;
  }
  auto existing_it = m_fs_cache[key_cache_id].find(cacheKey);
  if (existing_it != m_fs_cache[key_cache_id].end()) {
    auto *existing = existing_it->second;
    NdbMutex_Lock(existing->m_waitLock);
    if (existing->m_state != FSCacheEntry::IS_INVALID) {
      // Entry is IS_VALID or IS_FILLING — nothing to do
      NdbMutex_Unlock(existing->m_waitLock);
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      DEB_FS("Feature view %s already cached, skipping", cacheKey.c_str());
      return true;
    }
    // IS_INVALID entry (e.g., left over from a DELETE event) — remove it
    // so we can reload fresh metadata.
    if (existing->m_ref_count > 0) {
      // Still in use — can't remove yet, retry later
      NdbMutex_Unlock(existing->m_waitLock);
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      return false;
    }
    m_fs_cache[key_cache_id].erase(existing_it);
    NdbMutex_Unlock(existing->m_waitLock);
    NdbMutex_Lock(m_queueLock[key_cache_id]);
    remove_entry(existing, key_cache_id);
    NdbMutex_Unlock(m_queueLock[key_cache_id]);
    delete existing;
  }

  // Release the lock before doing the slow metadata fetch. We intentionally
  // do NOT create a cache entry here: if the load fails (e.g., dependent rows
  // not yet inserted), we simply return without caching the error. The
  // lazy-load path triggered by actual requests will retry later.
  NdbMutex_Unlock(m_rwLock[key_cache_id]);

  auto [data, errorCode] =
    metadata::GetFeatureViewMetadata(fsName, fvName, fvVersion);

  if (data == nullptr) {
    g_eventLogger->warning("[FS Cache] Failed to preload feature view %s: %s",
                           cacheKey.c_str(),
                           errorCode ? errorCode->ToString().c_str()
                                     : "unknown error");
    DEB_FS("Preload failed for %s, not caching error", cacheKey.c_str());
    return false;
  }

  // Load succeeded — insert into cache if no one else created an entry
  // while we were loading (lock was released during fetch).
  // Three possible states of the map entry:
  //   (a) No entry:  normal case, insert below.
  //   (b) IS_INVALID with ref_count==0: stale error from a lazy-load race.
  //       Replace it with our fresh valid data.
  //   (c) IS_VALID, IS_FILLING, or referenced: another path populated it.
  //       Discard our duplicate data.
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  if (m_stopped) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    delete data;
    return true;
  }
  auto race_it = m_fs_cache[key_cache_id].find(cacheKey);
  if (race_it != m_fs_cache[key_cache_id].end()) {
    auto *existing = race_it->second;
    NdbMutex_Lock(existing->m_waitLock);
    if (existing->m_state == FSCacheEntry::IS_INVALID &&
        existing->m_ref_count == 0) {
      // Case (b): Replace stale IS_INVALID entry with fresh valid data.
      m_fs_cache[key_cache_id].erase(race_it);
      NdbMutex_Unlock(existing->m_waitLock);
      NdbMutex_Lock(m_queueLock[key_cache_id]);
      remove_entry(existing, key_cache_id);
      NdbMutex_Unlock(m_queueLock[key_cache_id]);
      delete existing;
      // Fall through to insert the new entry below.
    } else {
      // Case (c): IS_VALID, IS_FILLING, or still referenced — keep it.
      NdbMutex_Unlock(existing->m_waitLock);
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      delete data;
      DEB_FS("Feature view %s populated by another path, discarding",
             cacheKey.c_str());
      return true;
    }
  }

  auto *newEntry = new FSCacheEntry();
  newEntry->m_ref_count = 0;
  newEntry->m_key_cache_id = key_cache_id;
  newEntry->m_key = cacheKey;
  newEntry->m_data = data;
  newEntry->m_errorCode = nullptr;
  newEntry->m_state = FSCacheEntry::IS_VALID;
  m_fs_cache[key_cache_id][cacheKey] = newEntry;

  NdbMutex_Lock(m_queueLock[key_cache_id]);
  newEntry->m_lastUsed = NdbTick_getCurrentTicks();
  insert_last(newEntry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);

  DEB_FS("Preloaded feature view: %s", cacheKey.c_str());
  return true;
}

void FSMetadataCache::preload_all_feature_views() {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != SUCCESS) {
    g_eventLogger->warning(
      "[FS Cache] Failed to scan feature_view table: %s", status.message);
    return;
  }

  Uint32 num_threads =
    globalConfigs.featureStore.featureStoreMetadataCache.preloadThreads;
  if (num_threads <= 1 || count <= 1) {
    num_threads = 1;
    for (int i = 0; i < count; i++) {
      char fs_name_buf[FEATURE_STORE_NAME_SIZE];
      RS_Status rs = find_feature_store_data(entries[i].feature_store_id,
                                             fs_name_buf);
      if (rs.http_code != SUCCESS) {
        g_eventLogger->warning(
          "[FS Cache] Failed to resolve feature store id %d, "
          "skipping fv id %d",
          entries[i].feature_store_id, entries[i].id);
        continue;
      }
      std::string fsName(fs_name_buf);
      std::string fvName(entries[i].name);
      int fvVersion = entries[i].version;
      load_single_feature_view(fsName, fvName, fvVersion);
    }
  } else {
    if (num_threads > (Uint32)count) {
      num_threads = (Uint32)count;
    }
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (Uint32 t = 0; t < num_threads; t++) {
      threads.emplace_back([this, entries, count, t, num_threads]() {
        for (int i = (int)t; i < count; i += (int)num_threads) {
          char fs_name_buf[FEATURE_STORE_NAME_SIZE];
          RS_Status rs = find_feature_store_data(
            entries[i].feature_store_id, fs_name_buf);
          if (rs.http_code != SUCCESS) {
            g_eventLogger->warning(
              "[FS Cache] Failed to resolve feature store id %d, "
              "skipping fv id %d",
              entries[i].feature_store_id, entries[i].id);
            continue;
          }
          std::string fsName(fs_name_buf);
          std::string fvName(entries[i].name);
          int fvVersion = entries[i].version;
          load_single_feature_view(fsName, fvName, fvVersion);
        }
      });
    }
    for (auto &th : threads) {
      th.join();
    }
  }
  free(entries);
  g_eventLogger->info("[FS Cache] Preloaded %d feature views using %u threads",
                      count, num_threads);
}

void FSMetadataCache::add_pending_insert(const std::string &fsName,
                                         const std::string &fvName,
                                         int fvVersion) {
  std::string cacheKey =
    metadata::getFeatureViewCacheKey(fsName, fvName, fvVersion);

  // Check for duplicate
  for (auto &p : m_pending_inserts) {
    if (p.cache_key == cacheKey) {
      // Reset retry so we try again soon
      p.retry_count = 0;
      p.polls_until_retry = 1;
      return;
    }
  }
  // Evict oldest entry if at capacity
  if ((int)m_pending_inserts.size() >= MAX_PENDING_INSERTS) {
    g_eventLogger->warning(
      "[FS Cache Event] Pending insert list full (%d), dropping oldest entry %s",
      MAX_PENDING_INSERTS, m_pending_inserts.front().cache_key.c_str());
    m_pending_inserts.erase(m_pending_inserts.begin());
  }
  m_pending_inserts.push_back({cacheKey, fsName, fvName, fvVersion, 0, 1});
}

void FSMetadataCache::remove_pending_insert(const std::string &cacheKey) {
  for (auto it = m_pending_inserts.begin(); it != m_pending_inserts.end();
       ++it) {
    if (it->cache_key == cacheKey) {
      DEB_FS("Removed %s from pending retry list", cacheKey.c_str());
      m_pending_inserts.erase(it);
      return;
    }
  }
}

void FSMetadataCache::process_pending_inserts() {
  int loads_this_cycle = 0;
  auto it = m_pending_inserts.begin();
  while (it != m_pending_inserts.end()) {
    if (m_stopped) break;

    it->polls_until_retry--;
    if (it->polls_until_retry > 0) {
      ++it;
      continue;
    }

    // Cap the number of loads per poll cycle to keep the event loop
    // responsive.  Each load_single_feature_view call does full NDB scans
    // that can take hundreds of milliseconds.  Without a cap, a large
    // pending list (e.g. after initial data loading) blocks pollEvents()
    // for tens of seconds, delaying DELETE event processing.
    if (loads_this_cycle >= MAX_RETRIES_PER_CYCLE) {
      it->polls_until_retry = 1;  // try again next cycle
      ++it;
      continue;
    }

    bool success = load_single_feature_view(
      it->fs_name, it->fv_name, it->fv_version);
    loads_this_cycle++;
    if (success) {
      it = m_pending_inserts.erase(it);
    } else {
      it->retry_count++;
      // Exponential backoff: 1, 2, 4, 8, ... capped at MAX_RETRY_POLLS
      int backoff = 1 << std::min(it->retry_count, 6);  // cap shift to avoid overflow
      it->polls_until_retry = std::min(backoff, MAX_RETRY_POLLS);
      DEB_FS("Deferred load failed for %s, next retry in %d polls (attempt %d)",
             it->cache_key.c_str(), it->polls_until_retry, it->retry_count);
      ++it;
    }
  }
}

extern "C" void* fs_event_thread_main(void *arg) {
  errno = 0;
  ((FSMetadataCache*)arg)->event_watcher_job();
  return nullptr;
}

void FSMetadataCache::start_event_watcher() {
  m_event_watcher_thread = NdbThread_Create(fs_event_thread_main,
                                             (NDB_THREAD_ARG *)this,
                                             128 * 1024,
                                             "FS Cache Event",
                                             NDB_THREAD_PRIO_LOW);
  if (m_event_watcher_thread == nullptr) {
    g_eventLogger->warning(
      "[FS Cache] Failed to start event watcher thread");
  }
}

void FSMetadataCache::evict_entry(const std::string &cacheKey) {
#if (NUM_FS_CACHES == 1)
  Uint32 key_cache_id = 0;
#else
  Uint32 hash = rondb_xxhash_std(cacheKey.c_str(), cacheKey.size());
  Uint32 key_cache_id = hash & (NUM_FS_CACHES - 1);
#endif

  NdbMutex_Lock(m_rwLock[key_cache_id]);
  auto it = m_fs_cache[key_cache_id].find(cacheKey);
  if (it == m_fs_cache[key_cache_id].end()) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    return;
  }
  auto *entry = it->second;
  NdbMutex_Lock(entry->m_waitLock);

  if (entry->m_ref_count > 0 ||
      entry->m_state == FSCacheEntry::IS_FILLING) {
    // In use or being loaded — mark invalid so it won't be served again.
    entry->m_state = FSCacheEntry::IS_INVALID;
    if (entry->m_errorCode == nullptr) {
      entry->m_errorCode = FV_NOT_EXIST->NewMessage(
        "Feature view was deleted");
    }
    entry->m_lastUsed = NdbTick_getCurrentTicks();
    NdbCondition_Broadcast(entry->m_waitCond);
    NdbMutex_Unlock(entry->m_waitLock);
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    return;
  }

  // ref_count == 0 and not IS_FILLING: safe to delete immediately
  m_fs_cache[key_cache_id].erase(it);

  // Remove from linked list while still holding m_rwLock to prevent
  // cache_entry_updater from finding this entry via m_first_cache_entry.
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  remove_entry(entry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);

  NdbMutex_Unlock(entry->m_waitLock);
  delete entry;
}

void FSMetadataCache::event_watcher_job() {
  const char *EVENT_NAME = m_event_name.c_str();
  bool first_connect = true;
  Uint32 retry_sleep_ms = 1000;
  static const Uint32 MAX_RETRY_SLEEP_MS = 30000;

  Ndb *ndb = nullptr;
  NdbEventOperation *ev_op = nullptr;
  NdbDictionary::Dictionary *dict = nullptr;
  NdbRecAttr *id_val = nullptr;
  NdbRecAttr *name_val = nullptr;
  NdbRecAttr *fs_id_val = nullptr;
  NdbRecAttr *version_val = nullptr;
  NdbRecAttr *name_pre_val = nullptr;
  NdbRecAttr *fs_id_pre_val = nullptr;
  NdbRecAttr *version_pre_val = nullptr;

retry:
  ndb = nullptr;
  ev_op = nullptr;
  dict = nullptr;

  if (m_stopped) goto done;

  {
    RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
    if (rs.http_code != SUCCESS) {
      g_eventLogger->warning(
        "[FS Cache Event] Failed to get NDB object. Retry...");
      goto err;
    }
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    g_eventLogger->warning(
      "[FS Cache Event] Failed to set database: %d(%s). Retry...",
      ndb->getNdbError().code, ndb->getNdbError().message);
    goto err;
  }

  dict = ndb->getDictionary();
  {
    dict->invalidateTable(FEATURE_VIEW);
    const NdbDictionary::Table *tab = dict->getTable(FEATURE_VIEW);
    if (tab == nullptr) {
      g_eventLogger->warning(
        "[FS Cache Event] Failed to get feature_view table: %d(%s). Retry...",
        dict->getNdbError().code, dict->getNdbError().message);
      goto err;
    }

    NdbDictionary::Event event(EVENT_NAME);
    event.setTable(*tab);
    event.addTableEvent(NdbDictionary::Event::TE_INSERT);
    event.addTableEvent(NdbDictionary::Event::TE_DELETE);
    event.mergeEvents(true);
    for (int col = 0; col < tab->getNoOfColumns(); col++) {
      event.addEventColumn(col);
    }

    dict->dropEvent(EVENT_NAME);

    if (dict->createEvent(event)) {
      g_eventLogger->warning(
        "[FS Cache Event] Failed to create event: %d(%s). Retry...",
        dict->getNdbError().code, dict->getNdbError().message);
      goto err;
    }
  }

  ev_op = ndb->createEventOperation(EVENT_NAME);
  if (ev_op == nullptr) {
    g_eventLogger->warning(
      "[FS Cache Event] Failed to create event operation: %d(%s). Retry...",
      ndb->getNdbError().code, ndb->getNdbError().message);
    goto err;
  }
  ev_op->mergeEvents(true);

  // Register PK column
  id_val = ev_op->getValue("id");
  (void)ev_op->getPreValue("id");

  name_val = ev_op->getValue("name");
  fs_id_val = ev_op->getValue("feature_store_id");
  version_val = ev_op->getValue("version");

  // Register all remaining columns so the event stream stays aligned.
  // created, creator, description — after-values and pre-values
  (void)ev_op->getValue("created");
  (void)ev_op->getValue("creator");
  (void)ev_op->getValue("description");

  name_pre_val = ev_op->getPreValue("name");
  fs_id_pre_val = ev_op->getPreValue("feature_store_id");
  version_pre_val = ev_op->getPreValue("version");

  (void)ev_op->getPreValue("created");
  (void)ev_op->getPreValue("creator");
  (void)ev_op->getPreValue("description");

  if (id_val == nullptr || name_val == nullptr ||
      fs_id_val == nullptr || version_val == nullptr ||
      name_pre_val == nullptr || fs_id_pre_val == nullptr ||
      version_pre_val == nullptr) {
    g_eventLogger->warning(
      "[FS Cache Event] Failed to register event columns "
      "(schema may have changed). Retry...");
    goto err;
  }

  if (ev_op->execute()) {
    g_eventLogger->warning(
      "[FS Cache Event] Failed to execute event operation: %d(%s). Retry...",
      ev_op->getNdbError().code, ev_op->getNdbError().message);
    goto err;
  }

  retry_sleep_ms = 1000;
  g_eventLogger->info("[FS Cache Event] Watcher %s",
                      first_connect ? "started" : "reconnected");

  if (!first_connect) {
    // When the event watcher hits an error it jumps to err:, tears down the
    // NDB event subscription, sleeps with exponential backoff (1s → 30s cap),
    // then jumps back to retry: to re-establish the subscription.  During
    // that gap any table events (INSERT/DELETE) are silently lost.
    //
    // Preload picks up missed INSERTs (load_single_feature_view skips
    // already-cached entries, so this is a fast no-op for most views).
    preload_all_feature_views();
    // Stale pending retries from before the disconnect are now redundant —
    // preload has already tried to load every known feature view.
    m_pending_inserts.clear();
  }
  first_connect = false;

  while (!m_stopped) {
    if (m_force_reconnect.exchange(false)) {
      g_eventLogger->info("[FS Cache Event] Forced reconnect requested");
      goto err;
    }
    int res = ndb->pollEvents(1000);
    if (res < 0) {
      g_eventLogger->warning(
        "[FS Cache Event] pollEvents error: %d(%s). Retry...",
        ndb->getNdbError().code, ndb->getNdbError().message);
      goto err;
    }
    if (res == 0) {
      if (!m_pending_inserts.empty()) {
        process_pending_inserts();
      }
      continue;
    }

    NdbEventOperation *op;
    while ((op = ndb->nextEvent())) {
      if (op->hasError()) {
        g_eventLogger->warning(
          "[FS Cache Event] Event error: %d(%s). Retry...",
          op->getNdbError().code, op->getNdbError().message);
        goto err;
      }

      switch (op->getEventType()) {
        case NdbDictionary::Event::TE_INSERT: {
          Uint32 name_bytes = 0;
          const char *name_start = nullptr;
          if (GetByteArray(name_val, &name_start, &name_bytes) != 0) {
            break;
          }
          std::string fvName(name_start, name_bytes);
          int fs_id = fs_id_val->int32_value();
          int version = version_val->int32_value();

          char fs_name_buf[FEATURE_STORE_NAME_SIZE];
          RS_Status rs = find_feature_store_data(fs_id, fs_name_buf);
          if (rs.http_code != SUCCESS) {
            g_eventLogger->warning(
              "[FS Cache Event] INSERT: failed to resolve feature_store_id %d",
              fs_id);
            break;
          }
          std::string fsName(fs_name_buf);
          if (!load_single_feature_view(fsName, fvName, version)) {
            add_pending_insert(fsName, fvName, version);
          }
          break;
        }
        case NdbDictionary::Event::TE_DELETE: {
          Uint32 name_bytes = 0;
          const char *name_start = nullptr;
          if (GetByteArray(name_pre_val, &name_start, &name_bytes) != 0) {
            break;
          }
          std::string fvName(name_start, name_bytes);
          int fs_id = fs_id_pre_val->int32_value();
          int version = version_pre_val->int32_value();

          char fs_name_buf[FEATURE_STORE_NAME_SIZE];
          RS_Status rs = find_feature_store_data(fs_id, fs_name_buf);
          if (rs.http_code != SUCCESS) {
            g_eventLogger->warning(
              "[FS Cache Event] DELETE: failed to resolve feature_store_id %d",
              fs_id);
            break;
          }
          std::string fsName(fs_name_buf);
          std::string cacheKey =
            metadata::getFeatureViewCacheKey(fsName, fvName, version);
          evict_entry(cacheKey);
          remove_pending_insert(cacheKey);
          break;
        }
        case NdbDictionary::Event::TE_CLUSTER_FAILURE:
        case NdbDictionary::Event::TE_STOP:
        case NdbDictionary::Event::TE_INCONSISTENT:
        case NdbDictionary::Event::TE_OUT_OF_MEMORY:
          g_eventLogger->warning(
            "[FS Cache Event] System event %d received. Retry...",
            op->getEventType());
          goto err;
        default:
          break;
      }
    }
    if (!m_pending_inserts.empty()) {
      process_pending_inserts();
    }
  }
  goto done;

err:
  if (ev_op != nullptr) {
    ndb->dropEventOperation(ev_op);
    ev_op = nullptr;
  }
  if (dict != nullptr) {
    dict->dropEvent(EVENT_NAME);
    dict = nullptr;
  }
  if (ndb != nullptr) {
    RS_Status rs;
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    ndb = nullptr;
  }
  if (!m_stopped) {
    g_eventLogger->info(
      "[FS Cache Event] Retrying in %u ms...", retry_sleep_ms);
    NdbMutex_Lock(m_sleepLock);
    if (!m_stopped)
      NdbCondition_WaitTimeout(m_sleepCond, m_sleepLock, retry_sleep_ms);
    NdbMutex_Unlock(m_sleepLock);
    retry_sleep_ms = std::min(retry_sleep_ms * 2, MAX_RETRY_SLEEP_MS);
    goto retry;
  }

done:
  if (ev_op != nullptr) {
    ndb->dropEventOperation(ev_op);
  }
  if (dict != nullptr) {
    dict->dropEvent(EVENT_NAME);
  }
  if (ndb != nullptr) {
    RS_Status rs;
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  }
  g_eventLogger->info("[FS Cache Event] Watcher stopped");
}
