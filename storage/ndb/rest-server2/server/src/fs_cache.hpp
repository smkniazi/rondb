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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_

#include "rdrs_hopsworks_dal.h"
#include "pk_data_structs.hpp"
#include "metadata.hpp"
#include "feature_store_error_code.hpp"

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <chrono>
#include <ndb_init.h>
#include <ndb_types.h>
#include <NdbTick.h>
#include <NdbSleep.h>
#include <NdbMutex.h>
#include <NdbCondition.h>
#include <NdbThread.h>

#define NUM_FS_CACHES 1

class FSMetadataCache;
extern FSMetadataCache *g_fs_metadata_cache;

void start_fs_cache();
void stop_fs_cache();
void fs_cache_dec_ref_count(char*);

class FSCacheEntry {
 public:
  metadata::FeatureViewMetadata *m_data;
  std::shared_ptr<RestErrorCode> m_errorCode;
  FSCacheEntry* m_next_cache_entry;
  FSCacheEntry* m_prev_cache_entry;
  NDB_TICKS m_lastUsed;
  NdbMutex *m_waitLock;
  NdbCondition *m_waitCond;
  Uint32 m_key_cache_id;
  std::string m_key;
  enum {
    IS_FILLING = 0,
    IS_INVALID = 1,
    IS_VALID = 2
  };
  Uint8 m_state;
  std::atomic<int> m_ref_count;
  // Set to true after evict_failed_entry() removes this entry from the
  // cache map.  When the last holder calls fs_cache_dec_ref_count() and
  // sees prev==1 && m_evicted, it deletes the entry.
  std::atomic<bool> m_evicted{false};

  FSCacheEntry() {
    m_data = nullptr;
    m_errorCode = nullptr;
    m_state = IS_FILLING;
    m_evicted = false;
    m_waitLock = NdbMutex_Create();
    m_waitCond = NdbCondition_Create();
  }

  ~FSCacheEntry() {
    NdbMutex_Destroy(m_waitLock);
    NdbCondition_Destroy(m_waitCond);
    if (m_data) {
      for (auto& [key, val] : m_data->complexFeatures) {
        val.unregister_with_go_layer();
      }
      delete m_data;
    }
  }
};

metadata::FeatureViewMetadata*
  fs_metadata_cache_get(const std::string&, FSCacheEntry**);
void fs_metadata_update_cache(metadata::FeatureViewMetadata*,
                              FSCacheEntry*,
                              std::shared_ptr<RestErrorCode>);
// Remove a failed lazy-load entry from the cache map so the next request
// creates a fresh IS_FILLING entry and retries.  Called after update_cache()
// has broadcast the error to waiting threads.  The entry is not deleted here;
// m_evicted is set so fs_cache_dec_ref_count() deletes it when the last
// reference is released.
void fs_metadata_evict_failed_entry(FSCacheEntry*);

class FSMetadataCache {
 public:
  FSMetadataCache();
  ~FSMetadataCache() {
    cleanup();
    for (int i = 0; i < NUM_FS_CACHES; i++) {
      NdbMutex_Destroy(m_rwLock[i]);
      NdbMutex_Destroy(m_queueLock[i]);
    }
    NdbMutex_Destroy(m_sleepLock);
    NdbCondition_Destroy(m_sleepCond);
  }

  metadata::FeatureViewMetadata*
    get_fs_metadata(const std::string&, FSCacheEntry**);
  void update_cache(metadata::FeatureViewMetadata*,
                    FSCacheEntry*,
                    std::shared_ptr<RestErrorCode>);
  void cache_entry_updater(Uint32);
  void start_fs_cache_thread();

  void preload_all_feature_views();
  void start_event_watcher();
  void event_watcher_job();
  // Remove a failed lazy-load entry from the cache map.  See
  // fs_metadata_evict_failed_entry() above for the full contract.
  void evict_failed_entry(FSCacheEntry *entry);
  // Force the event watcher to tear down and reconnect (for testing)
  void force_reconnect() { m_force_reconnect = true; }

 private:
  std::unordered_map<std::string, FSCacheEntry*> m_fs_cache[NUM_FS_CACHES];
  std::atomic<bool> m_stopped{false};
  std::atomic<bool> m_force_reconnect{false};
  NdbMutex *m_rwLock[NUM_FS_CACHES];
  NdbMutex *m_queueLock[NUM_FS_CACHES];
  NdbMutex *m_sleepLock;
  NdbCondition *m_sleepCond;
  FSCacheEntry* m_first_cache_entry[NUM_FS_CACHES];
  FSCacheEntry* m_last_cache_entry[NUM_FS_CACHES];
  NdbThread* m_cache_threads[NUM_FS_CACHES];
  NdbThread* m_event_watcher_thread;
  std::string m_event_name;
  bool m_is_thread_running;

  void cleanup();
  FSCacheEntry* allocate_empty_cache_entry(const std::string &fs_key,
                                           const Uint32 key_cache_id);
  void insert_last(FSCacheEntry*, Uint32);
  void remove_entry(FSCacheEntry*, Uint32);

  // Fetch metadata from NDB and insert into cache.  Called from both the
  // event watcher (INSERT events) and preload path.  Returns false if the
  // NDB fetch failed (caller should add to pending retry list), true on
  // success or if the entry was already cached by another path.
  bool load_single_feature_view(const std::string &fsName,
                                const std::string &fvName,
                                int fvVersion);
  void evict_entry(const std::string &cacheKey);

  // Deferred retry for INSERT events that fail because dependent metadata
  // rows don't exist yet (e.g. during backup/restore when feature_view
  // is restored before training_dataset_join).  Retries with exponential
  // backoff (1s, 2s, 4s, ... capped at MAX_RETRY_POLLS seconds).
  // Only accessed from the event watcher thread — no locking needed.
  struct PendingInsert {
    std::string cache_key;
    std::string fs_name;
    std::string fv_name;
    int fv_version;
    int retry_count;       // number of failed attempts so far
    int polls_until_retry; // poll cycles to skip before next attempt
  };
  std::vector<PendingInsert> m_pending_inserts;
  static constexpr int MAX_PENDING_INSERTS = 1000;  // bounded list size
  static constexpr int MAX_RETRY_POLLS = 60;        // backoff cap (~60s)
  static constexpr int MAX_RETRIES_PER_CYCLE = 1;   // event loop starvation cap
  void process_pending_inserts();
  void add_pending_insert(const std::string &fsName,
                          const std::string &fvName,
                          int fvVersion);
  void remove_pending_insert(const std::string &cacheKey);
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_
