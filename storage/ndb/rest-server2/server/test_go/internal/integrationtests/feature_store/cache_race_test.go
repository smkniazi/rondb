/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package feature_store

// Tests for RONDB-1030: Feature View Metadata Cache race condition.
//
// The NDB event watcher on `feature_view` fires an INSERT event before
// dependent metadata rows (training_dataset_join, training_dataset_feature,
// serving_key, schemas, subjects) exist in their respective tables.  This
// happens during backup/restore and test setup where SQL statements execute
// one at a time with FK checks disabled.
//
// Two fixes are validated:
//   Fix 1 – Lazy-load errors are no longer permanently cached (evicted
//           immediately, so the next request retries from scratch).
//   Fix 2 – Event watcher retries failed INSERTs with exponential backoff.
//
// All waits use polling helpers (pollSimpleUntilOK / pollSimpleUntilNotOK)
// that actively verify the expected HTTP status instead of sleeping a fixed
// duration.  This eliminates timing dependencies on event loop throughput
// and makes the tests deterministic regardless of machine speed.

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	fsmetadata "hopsworks.ai/rdrs2/internal/feature_store"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// ---------------------------------------------------------------------------
// SQL constants – row values taken verbatim from hopsworks_40_data.sql.
//
// FV 2059: "sample_1" in FSDB001 (feature_store_id=67).
//   Single-table FV with one feature group (fg=2069) and one serving key.
// ---------------------------------------------------------------------------
const (
	fsNameSimple    = "fsdb001"
	fvNameSimple    = "sample_1"
	fvVersionSimple = 1

	sqlInsertFV2059 = `INSERT INTO hopsworks.feature_view VALUES
		(2059, 'sample_1', 67, Timestamp('2023-04-21 09:52:51'), 10000, 1, '');`

	sqlDeleteFV2059 = `DELETE FROM hopsworks.feature_view WHERE id = 2059;`

	sqlInsertTDJ2051 = `INSERT INTO hopsworks.training_dataset_join VALUES
		(2051, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2059);`

	sqlInsertTDF2059 = `INSERT INTO hopsworks.training_dataset_feature VALUES
		(2057, NULL, 2069, 'data1', 'bigint', 2051, 2, 0, 0, 0, 2059, NULL),
		(2058, NULL, 2069, 'id1', 'bigint', 2051, 0, 0, 0, 0, 2059, NULL),
		(2059, NULL, 2069, 'ts', 'timestamp', 2051, 1, 0, 0, 0, 2059, NULL),
		(2060, NULL, 2069, 'data2', 'bigint', 2051, 3, 0, 0, 0, 2059, NULL);`

	sqlInsertSK68 = `INSERT INTO hopsworks.serving_key VALUES
		(68, NULL, 'id1', NULL, 0, 2069, 1, 2059);`

	sqlInsertAllDeps2059 = sqlInsertTDJ2051 + "\n" + sqlInsertTDF2059 + "\n" + sqlInsertSK68

	// feature_group 2069 — the FG that FV 2059's TDJ/TDF rows reference.
	sqlDeleteFG2069 = `SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM hopsworks.feature_group WHERE id = 2069;
SET FOREIGN_KEY_CHECKS = 1;`

	sqlInsertFG2069 = `INSERT INTO hopsworks.feature_group VALUES
		(2069, 'sample_1', 67, Timestamp('2023-04-21 09:33:40'), 10000, 1, NULL, 2, NULL, NULL, 2057, 'ts', 1, NULL, NULL, FALSE, 0, NULL, NULL);`

	// Explicit dep deletion — NDB may not reliably CASCADE on feature_view
	// DELETE, so we remove children ourselves.  Delete order: children first,
	// then parent.  DELETEs with zero matching rows are harmless no-ops.
	sqlDeleteDeps2059 = `DELETE FROM hopsworks.serving_key WHERE feature_view_id = 2059;
DELETE FROM hopsworks.training_dataset_feature WHERE feature_view_id = 2059;
DELETE FROM hopsworks.training_dataset_join WHERE feature_view_id = 2059;`
)

// ---------------------------------------------------------------------------
// FV 23: "complex_example" in FSDB003 (feature_store_id=68).
//   Has complex features (array, struct) that require schemas+subjects.
// ---------------------------------------------------------------------------
const (
	fsNameComplex    = "fsdb003"
	fvNameComplex    = "complex_example"
	fvVersionComplex = 1

	sqlInsertFV23 = `INSERT INTO hopsworks.feature_view VALUES
		(23, 'complex_example', 68, Timestamp('2023-09-26 10:03:16'), 10000, 1, '');`

	sqlDeleteFV23 = `DELETE FROM hopsworks.feature_view WHERE id = 23;`

	sqlInsertTDJ29 = `INSERT INTO hopsworks.training_dataset_join VALUES
		(29, NULL, 35, NULL, NULL, 0, 0, 0, NULL, 23);`

	sqlInsertTDF23 = `INSERT INTO hopsworks.training_dataset_feature VALUES
		(70, NULL, 35, 'id', 'bigint', 29, 0, 0, 0, 0, 23, NULL),
		(71, NULL, 35, 'ts', 'bigint', 29, 1, 0, 0, 0, 23, NULL),
		(72, NULL, 35, 'array', 'array<bigint>', 29, 2, 0, 0, 0, 23, NULL),
		(73, NULL, 35, 'struct', 'struct<int1:bigint,int2:bigint>', 29, 3, 0, 0, 0, 23, NULL);`

	sqlInsertSK1523 = `INSERT INTO hopsworks.serving_key VALUES
		(1523, NULL, 'id', NULL, 0, 35, 1, 23);`

	sqlInsertSchema25 = `INSERT INTO hopsworks.schemas VALUES
		(25, '{"type":"record","name":"complex_example_1","namespace":"caps_featurestore.db","fields":[{"name":"id","type":["null","long"]},{"name":"ts","type":["null","long"]},{"name":"array","type":["null",{"type":"array","items":["null","long"]}]},{"name":"struct","type":["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]}]}', 1003);`

	sqlInsertSubject25 = `INSERT INTO hopsworks.subjects VALUES
		(25, 'complex_example_1', 1, 25, 1003, Timestamp('2023-09-27 10:02:58'));`

	sqlDeleteSchema25  = `DELETE FROM hopsworks.schemas WHERE id = 25;`
	sqlDeleteSubject25 = `DELETE FROM hopsworks.subjects WHERE id = 25;`

	sqlInsertAllDeps23 = sqlInsertTDJ29 + "\n" + sqlInsertTDF23 + "\n" + sqlInsertSK1523

	sqlDeleteDeps23 = `DELETE FROM hopsworks.serving_key WHERE feature_view_id = 23;
DELETE FROM hopsworks.training_dataset_feature WHERE feature_view_id = 23;
DELETE FROM hopsworks.training_dataset_join WHERE feature_view_id = 23;`
)

// ---------------------------------------------------------------------------
// Polling helpers — wait for the cache to reflect the expected state by
// repeatedly sending HTTP requests.  This replaces fixed-duration sleeps
// and is immune to event loop latency variations across machines.
// ---------------------------------------------------------------------------
const (
	pollTimeout  = 30 * time.Second
	pollInterval = 500 * time.Millisecond
)

// pollSimpleUntilNotOK polls the simple FV endpoint until the response is
// NOT 200 OK, indicating the cache entry has been evicted (e.g. after a
// DELETE event).
func pollSimpleUntilNotOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
			fvVersionSimple, "id1", "1")
		if status != http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatal("Timed out (30s) waiting for simple FV cache eviction")
}

// pollSimpleUntilOK polls the simple FV endpoint until it returns 200 OK,
// indicating the cache has been populated (e.g. by event watcher retry or
// a successful lazy-load).
func pollSimpleUntilOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var lastStatus int
	for time.Now().Before(deadline) {
		lastStatus, _ = sendRawFSRequest(t, fsNameSimple, fvNameSimple,
			fvVersionSimple, "id1", "1")
		if lastStatus == http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("Timed out (30s) waiting for simple FV 200 OK (last: %d)", lastStatus)
}

// pollComplexUntilNotOK polls the complex FV endpoint until the response is
// NOT 200 OK.
func pollComplexUntilNotOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		status, _ := sendRawFSRequest(t, fsNameComplex, fvNameComplex,
			fvVersionComplex, "id", "1")
		if status != http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatal("Timed out (30s) waiting for complex FV cache eviction")
}

// pollComplexUntilOK polls the complex FV endpoint until it returns 200 OK.
func pollComplexUntilOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var lastStatus int
	for time.Now().Before(deadline) {
		lastStatus, _ = sendRawFSRequest(t, fsNameComplex, fvNameComplex,
			fvVersionComplex, "id", "1")
		if lastStatus == http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("Timed out (30s) waiting for complex FV 200 OK (last: %d)", lastStatus)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func runSQL(t *testing.T, sql string) {
	t.Helper()
	err := testutils.RunQueriesOnMetadataCluster(sql)
	if err != nil {
		t.Fatalf("SQL execution failed: %v", err)
	}
}

// deleteSimpleFV removes FV 2059 and all its dependent rows.  Deps are
// deleted explicitly because NDB may not fire ON DELETE CASCADE reliably.
// Safe to call regardless of current DB state (no-op for missing rows).
func deleteSimpleFV(t *testing.T) {
	t.Helper()
	// Children first, then parent — avoids FK issues in either direction.
	runSQL(t, sqlDeleteDeps2059+"\n"+sqlDeleteFV2059)
}

// deleteComplexFV removes FV 23 and all its dependent rows.
func deleteComplexFV(t *testing.T) {
	t.Helper()
	runSQL(t, sqlDeleteDeps23+"\n"+sqlDeleteFV23)
}

// restoreSimpleFV brings FV 2059 and all its dependent rows back to the
// original state.  It first deletes (idempotent — no error if missing),
// then re-inserts everything.  Safe to call regardless of current DB state.
//
// IMPORTANT: Deps are inserted BEFORE the feature_view row (with FK checks
// disabled) to avoid a race condition.  The NDB event watcher fires on the
// feature_view INSERT and calls load_single_feature_view concurrently.
// If deps don't exist yet at that point, the load fails.  By inserting deps
// first, the event-triggered load always sees full data.
func restoreSimpleFV(t *testing.T) {
	t.Helper()
	// Explicitly delete deps + FV (NDB CASCADE is unreliable).
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	// Deps reference feature_view via FK, so disable FK checks to insert
	// them before the parent row.  All statements run on the same DB
	// connection, so the session variable persists across splits.
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	// Poll until the event watcher (or lazy-load) populates the cache.
	pollSimpleUntilOK(t)
}

// restoreComplexFV brings FV 23 and all its dependent rows (including
// schemas and subjects) back to the original state.
// See restoreSimpleFV for why deps are inserted before the feature_view row.
func restoreComplexFV(t *testing.T) {
	t.Helper()
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps23)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV23)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteSubject25)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteSchema25)
	// Schemas/subjects have no FK to feature_view, but we include them
	// inside the FK-disabled block for simplicity and ordering safety.
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertSchema25+"\n"+
		sqlInsertSubject25+"\n"+
		sqlInsertAllDeps23+"\n"+
		sqlInsertFV23+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollComplexUntilOK(t)
}

func makeSimpleFVRequest(t *testing.T, expectedMsg string, expectedStatus int) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		fsNameSimple, fvNameSimple, fvVersionSimple,
		[]string{"id1"},
		[]interface{}{[]byte("1")},
		nil, nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, expectedMsg, expectedStatus)
}

func makeComplexFVRequest(t *testing.T, expectedMsg string, expectedStatus int) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		fsNameComplex, fvNameComplex, fvVersionComplex,
		[]string{"id"},
		[]interface{}{[]byte("1")},
		nil, nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, expectedMsg, expectedStatus)
}

// sendRawFSRequest returns the HTTP status and response body without
// failing the test on unexpected status — the caller inspects it.
func sendRawFSRequest(t *testing.T, fsName, fvName string, fvVersion int,
	pk string, pkVal string) (int, string) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		fsName, fvName, fvVersion,
		[]string{pk},
		[]interface{}{[]byte(pkVal)},
		nil, nil,
	)
	body := fsReq.String()
	status, respBody := testclient.SendHttpRequest(t,
		config.FEATURE_STORE_HTTP_VERB,
		testutils.NewFeatureStoreURL(),
		body, "",
		http.StatusOK, http.StatusBadRequest,
		http.StatusNotFound, http.StatusInternalServerError)
	return status, string(respBody)
}

// ===========================================================================
// Group 1: Lazy-Load Error Recovery (Fix 1)
//
// Proves that when the lazy-load path encounters an error (missing dependent
// rows), the error is NOT permanently cached.  After the dependent rows are
// inserted, the next request succeeds.
// ===========================================================================

func Test_CacheRace_MissingJoins_Recovery(t *testing.T) {
	defer restoreSimpleFV(t)

	// Baseline
	makeSimpleFVRequest(t, "", http.StatusOK)

	// Delete FV + deps explicitly (NDB CASCADE unreliable)
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// INSERT only feature_view (no deps)
	runSQL(t, sqlInsertFV2059)

	// Request should fail — missing training_dataset_join → FG_NOT_EXIST
	makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

	// Insert all dependent rows
	runSQL(t, sqlInsertAllDeps2059)

	// Request should now succeed — error was NOT permanently cached
	makeSimpleFVRequest(t, "", http.StatusOK)
}

func Test_CacheRace_MissingFeatures_Recovery(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Re-insert feature_view + tdj only (no tdf, no sk)
	runSQL(t, sqlInsertFV2059)
	runSQL(t, sqlInsertTDJ2051)

	// Request should fail — missing training_dataset_feature.
	// The C++ code returns 404 from find_training_dataset_data_int, and
	// metadata.cpp maps "Not Found" errors to FG_NOT_EXIST.
	makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

	// Insert remaining deps
	runSQL(t, sqlInsertTDF2059)
	runSQL(t, sqlInsertSK68)

	// Should succeed now
	makeSimpleFVRequest(t, "", http.StatusOK)
}

func Test_CacheRace_MissingServingKey_Recovery(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Re-insert feature_view + tdj + tdf but NOT serving_key
	runSQL(t, sqlInsertFV2059)
	runSQL(t, sqlInsertTDJ2051)
	runSQL(t, sqlInsertTDF2059)

	// Missing serving_key → find_serving_key_data returns 404 → GetServingKeys
	// fails → FV_READ_FAIL("Failed to read serving keys.").
	// Fix 1 ensures this error is NOT permanently cached.
	makeSimpleFVRequest(t, fsmetadata.FV_READ_FAIL.GetReason(), http.StatusBadRequest)

	// Insert the serving key
	runSQL(t, sqlInsertSK68)

	// Should succeed now — the previous error was evicted, fresh load succeeds.
	makeSimpleFVRequest(t, "", http.StatusOK)
}

func Test_CacheRace_MultipleFailures_ThenSuccess(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert only feature_view
	runSQL(t, sqlInsertFV2059)

	// Multiple requests should all fail (proving error is not cached permanently)
	for i := 0; i < 3; i++ {
		makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}

	// Insert all deps
	runSQL(t, sqlInsertAllDeps2059)

	// Now should succeed
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// ===========================================================================
// Group 2: Complex Feature View (schemas/subjects dependency)
// ===========================================================================

func Test_CacheRace_ComplexFV_MissingSchema_Recovery(t *testing.T) {
	defer restoreComplexFV(t)

	makeComplexFVRequest(t, "", http.StatusOK)

	// Delete FV + deps explicitly (NDB CASCADE unreliable)
	deleteComplexFV(t)
	pollComplexUntilNotOK(t)

	// Also delete the schema and subject for this FV's feature group
	runSQL(t, sqlDeleteSubject25)
	runSQL(t, sqlDeleteSchema25)

	// Re-insert FV 23 with all structural deps but NO schema
	runSQL(t, sqlInsertFV23)
	runSQL(t, sqlInsertTDJ29)
	runSQL(t, sqlInsertTDF23)
	runSQL(t, sqlInsertSK1523)

	// Request should fail — missing schema for complex features
	makeComplexFVRequest(t, "", http.StatusBadRequest)

	// Restore schema + subject
	runSQL(t, sqlInsertSchema25)
	runSQL(t, sqlInsertSubject25)

	// Request should now succeed
	makeComplexFVRequest(t, "", http.StatusOK)
}

// ===========================================================================
// Group 3: Event Watcher Deferred Retry (Fix 2)
//
// These tests specifically verify the event watcher's retry mechanism, so
// they use time.Sleep to control the timing between INSERT events and dep
// insertion.  The final assertion does NOT use polling — it verifies that
// the cache was populated by the deferred retry (not by a lazy-load from
// a polling request).
// ===========================================================================

func Test_CacheRace_DeferredRetry_Success(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert feature_view only → event fires, load_single_feature_view fails,
	// entry added to m_pending_inserts
	runSQL(t, sqlInsertFV2059)

	// Wait for the event to fire and the initial load to fail
	time.Sleep(2 * time.Second)

	// Now insert all dependent rows
	runSQL(t, sqlInsertAllDeps2059)

	// Wait for the deferred retry to fire.  Backoff: polls_until_retry starts
	// at 1 (1s poll), then doubles.  Within 15s the retry should succeed.
	time.Sleep(15 * time.Second)

	// Request should succeed — cache populated by event watcher retry
	makeSimpleFVRequest(t, "", http.StatusOK)
}

func Test_CacheRace_DeferredRetry_DeleteCancelsPending(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert feature_view only → load fails, added to pending
	runSQL(t, sqlInsertFV2059)
	time.Sleep(2 * time.Second)

	// Delete the feature_view → should remove from pending + evict cache
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Request should get FV_NOT_EXIST (not a stale cached error)
	makeSimpleFVRequest(t, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

// ===========================================================================
// Group 4: Concurrent Access
// ===========================================================================

func Test_CacheRace_ConcurrentRequests_NoDeadlock(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert feature_view only (no deps)
	runSQL(t, sqlInsertFV2059)

	// Launch 10 concurrent requests — all should get errors, no deadlock/crash
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := sendRawFSRequest(t,
				fsNameSimple, fvNameSimple, fvVersionSimple, "id1", "1")
			if status == http.StatusOK {
				t.Errorf("Expected error status, got 200")
			}
		}()
	}
	wg.Wait()

	// Insert all deps
	runSQL(t, sqlInsertAllDeps2059)

	// Launch 10 concurrent requests — all should succeed
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, resp := sendRawFSRequest(t,
				fsNameSimple, fvNameSimple, fvVersionSimple, "id1", "1")
			if status != http.StatusOK {
				t.Errorf("Expected 200, got %d: %s", status, resp)
			}
		}()
	}
	wg.Wait()
}

// ===========================================================================
// Group 5: Edge Cases
// ===========================================================================

// Test_CacheRace_RapidDeleteInsertCycles performs multiple delete→insert→error→
// recover cycles in quick succession.  This exercises the MAX_RETRIES_PER_CYCLE
// cap: each cycle generates DELETE+INSERT NDB events and potentially adds entries
// to the pending retry list.  Without the cap, pending retries would block the
// event loop and stale cache entries would serve incorrect 200 responses.
func Test_CacheRace_RapidDeleteInsertCycles(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	for cycle := 0; cycle < 3; cycle++ {
		// Delete everything
		deleteSimpleFV(t)
		pollSimpleUntilNotOK(t)

		// Insert FV only — expect error
		runSQL(t, sqlInsertFV2059)
		makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

		// Insert deps — expect success
		runSQL(t, sqlInsertAllDeps2059)
		makeSimpleFVRequest(t, "", http.StatusOK)
	}
}

// Test_CacheRace_DeleteEvictsPromptly verifies that after a DELETE, the cache
// entry is evicted and subsequent requests do not serve stale IS_VALID data.
// The MAX_RETRIES_PER_CYCLE cap keeps the event loop responsive so DELETE
// events are processed promptly even when pending retries exist.
func Test_CacheRace_DeleteEvictsPromptly(t *testing.T) {
	defer restoreSimpleFV(t)

	// Prime the cache
	makeSimpleFVRequest(t, "", http.StatusOK)

	// Delete FV + deps — poll until the cache reflects the deletion.
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Verify the expected error — FV no longer exists.
	makeSimpleFVRequest(t, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

// Test_CacheRace_BackToBackRecovery exercises two consecutive error→recovery
// cycles without any explicit delay between them.  This verifies that
// evict_failed_entry properly clears the cache between cycles and that the
// pending retry list doesn't carry stale entries from cycle 1 into cycle 2.
func Test_CacheRace_BackToBackRecovery(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// --- Cycle 1 ---
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	runSQL(t, sqlInsertFV2059)
	makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

	runSQL(t, sqlInsertAllDeps2059)
	makeSimpleFVRequest(t, "", http.StatusOK)

	// --- Cycle 2 (immediate, no extra delay) ---
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	runSQL(t, sqlInsertFV2059)

	// Must still fail — the error from cycle 1 must not be cached, and the
	// pending retry from cycle 1 must not interfere.
	makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

	runSQL(t, sqlInsertAllDeps2059)
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// Test_CacheRace_ComplexFV_PartialDeps_Recovery verifies that a complex FV
// (with array/struct types requiring Avro schemas) fails gracefully when
// structural deps (TDJ/TDF/SK) exist but the schema is missing, and recovers
// once the schema is restored.
func Test_CacheRace_ComplexFV_PartialDeps_Recovery(t *testing.T) {
	defer restoreComplexFV(t)

	makeComplexFVRequest(t, "", http.StatusOK)

	// Delete FV + deps + schema/subject
	deleteComplexFV(t)
	pollComplexUntilNotOK(t)
	runSQL(t, sqlDeleteSubject25)
	runSQL(t, sqlDeleteSchema25)

	// Re-insert everything EXCEPT schema/subject
	runSQL(t, sqlInsertFV23)
	runSQL(t, sqlInsertTDJ29)
	runSQL(t, sqlInsertTDF23)
	runSQL(t, sqlInsertSK1523)

	// Should fail — complex features need the Avro schema
	makeComplexFVRequest(t, "", http.StatusBadRequest)

	// Restore schema + subject → should succeed
	runSQL(t, sqlInsertSchema25)
	runSQL(t, sqlInsertSubject25)
	makeComplexFVRequest(t, "", http.StatusOK)
}

func Test_CacheRace_ServingKeyZeroRows(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert FV + tdj + tdf but NOT serving_key
	runSQL(t, sqlInsertFV2059)
	runSQL(t, sqlInsertTDJ2051)
	runSQL(t, sqlInsertTDF2059)

	// Missing serving_key → find_serving_key_data returns 404 → GetServingKeys
	// fails → FV_READ_FAIL("Failed to read serving keys.").
	// Fix 1 ensures the error is NOT permanently cached.
	makeSimpleFVRequest(t, fsmetadata.FV_READ_FAIL.GetReason(), http.StatusBadRequest)

	// Insert the serving key and verify recovery.
	runSQL(t, sqlInsertSK68)
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// Test_CacheRace_MissingFeatureGroup_Recovery validates the case where the
// feature_group row itself is missing.  During backup/restore the feature_group
// table may be restored AFTER feature_view and its direct children (TDJ, TDF,
// SK).  GetFeatureViewMetadata iterates TDF rows and calls GetFeatureGroupData()
// for each referenced FG — a 404 there returns FG_NOT_EXIST.
// Fix 1 ensures the error is not permanently cached.
func Test_CacheRace_MissingFeatureGroup_Recovery(t *testing.T) {
	defer func() {
		// FG 2069 may be missing — insert it first (ignore dup-key errors),
		// then restore the FV + deps normally.
		_ = testutils.RunQueriesOnMetadataCluster(sqlInsertFG2069)
		restoreSimpleFV(t)
	}()

	// Baseline
	makeSimpleFVRequest(t, "", http.StatusOK)

	// Remove FV + deps
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Remove the feature_group row (FK_CHECKS=0 to avoid cascading to
	// other tables that reference feature_group).
	runSQL(t, sqlDeleteFG2069)

	// Re-insert FV + all direct deps — they reference FG 2069 which is now
	// gone, so FK checks must be disabled for the TDJ/TDF/SK inserts.
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertFV2059+"\n"+
		sqlInsertTDJ2051+"\n"+
		sqlInsertTDF2059+"\n"+
		sqlInsertSK68+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// Request should fail — GetFeatureGroupData(2069) returns 404 → FG_NOT_EXIST
	makeSimpleFVRequest(t, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)

	// Restore the feature_group row
	runSQL(t, sqlInsertFG2069)

	// Should succeed now — error was not permanently cached
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// ===========================================================================
// Assumption Tests — Diagnostic tests that validate core assumptions about
// NDB event behavior.  Run with:
//   ./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/feature_store TestAssumption
//
// The pass/fail pattern directly identifies the broken assumption:
//
//   Test                             | Fails if...
//   ---------------------------------|--------------------------------------------
//   StandaloneDelete                 | Event watcher doesn't process DELETE at all
//   StandaloneInsert                 | Event watcher doesn't process INSERT at all
//   RestoreThenDeleteImmediate       | Event merging eats DELETE after quick restore
//   RestoreThenDeleteWithGap         | Something OTHER than event merging is broken
//   InsertThenDeleteSameGCI          | INSERT+DELETE in same GCI → no event (merged)
//   DeleteThenInsertSameGCI          | DELETE+INSERT in same GCI → UPDATE (not subscribed)
//
// Key inference:
//   If RestoreThenDeleteImmediate FAILS but RestoreThenDeleteWithGap PASSES,
//   then NDB event merging (mergeEvents=true) is confirmed as the root cause.
// ===========================================================================

// TestAssumption_StandaloneDelete verifies the most basic event flow:
// SQL DELETE on feature_view → NDB TE_DELETE event → evict_entry → cache cleared.
// If this fails, the event watcher is fundamentally broken.
func TestAssumption_StandaloneDelete(t *testing.T) {
	defer restoreSimpleFV(t)

	// Ensure cached
	makeSimpleFVRequest(t, "", http.StatusOK)

	// Standalone delete — no preceding INSERT in this test
	deleteSimpleFV(t)

	// Must see non-200 within 30s.  Failure = event watcher is dead.
	pollSimpleUntilNotOK(t)
}

// TestAssumption_StandaloneInsert verifies that an INSERT on feature_view
// causes the event watcher to load the metadata WITHOUT any client request
// triggering a lazy-load.  We sleep instead of polling to isolate event
// watcher behavior from the lazy-load path.
func TestAssumption_StandaloneInsert(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert deps first, then FV — event should trigger successful load
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// Wait WITHOUT making any requests — give the event watcher time to load.
	// 5 seconds >> GCI interval (~2s) + poll timeout (1s) + load time (~1s).
	time.Sleep(5 * time.Second)

	// Single request — should be 200 if event watcher loaded it.
	// Failure = INSERT events don't trigger loading.
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// TestAssumption_RestoreThenDeleteImmediate exactly replicates the pattern
// from restoreSimpleFV (DELETE + INSERT) followed immediately by deleteSimpleFV.
// This is the pattern that fails in the other tests.
//
// If this FAILS (30s timeout at pollSimpleUntilNotOK), the DELETE event from
// the second deleteSimpleFV is being swallowed — likely by NDB event merging
// with the INSERT event from restoreSimpleFV (both in the same GCI).
func TestAssumption_RestoreThenDeleteImmediate(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// --- Simulate restoreSimpleFV (DELETE + INSERT in quick succession) ---
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	// --- Immediately delete (like the next test would) ---
	deleteSimpleFV(t)

	// If this times out → event merging is the root cause.
	pollSimpleUntilNotOK(t)
}

// TestAssumption_RestoreThenDeleteWithGap is identical to the above but adds
// a 5-second gap between restore and delete.  This ensures the INSERT event
// from restoreSimpleFV is in a DIFFERENT GCI from the DELETE event.
//
// Compare results:
//   Immediate FAILS + WithGap PASSES → event merging confirmed
//   Both FAIL → NOT event merging, something else is broken
func TestAssumption_RestoreThenDeleteWithGap(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// --- Simulate restoreSimpleFV ---
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	// 5-second gap: ensures the INSERT event is consumed by the event watcher
	// and that a GCI boundary passes before the next DELETE.
	t.Log("Waiting 5s for GCI boundary...")
	time.Sleep(5 * time.Second)

	// --- Now delete ---
	deleteSimpleFV(t)

	// If this ALSO times out → root cause is NOT event merging.
	pollSimpleUntilNotOK(t)
}

// TestAssumption_InsertThenDeleteSameGCI tests INSERT followed by DELETE in
// the tightest possible timing (single SQL batch on same connection).
// With mergeEvents(true), INSERT + DELETE within the same GCI → no event.
func TestAssumption_InsertThenDeleteSameGCI(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// First clear the cache
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// INSERT + DELETE in a single SQL batch — maximum chance of same GCI
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		sqlDeleteDeps2059+"\n"+
		sqlDeleteFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// Wait for event processing
	time.Sleep(5 * time.Second)

	// FV should NOT be in cache (it was deleted).
	// If this returns 200 → INSERT event loaded it and DELETE event was merged away.
	status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
		fvVersionSimple, "id1", "1")
	if status == http.StatusOK {
		t.Log("RESULT: INSERT+DELETE same batch → cache still shows 200 → DELETE event was MERGED AWAY")
	} else {
		t.Logf("RESULT: INSERT+DELETE same batch → cache shows %d → events processed correctly", status)
	}
	// Don't fail — this is diagnostic
}

// TestAssumption_DeleteThenInsertSameGCI tests DELETE followed by INSERT in
// the tightest possible timing.
// With mergeEvents(true), DELETE + INSERT within the same GCI → UPDATE (not subscribed).
func TestAssumption_DeleteThenInsertSameGCI(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// DELETE + INSERT in a single SQL batch — maximum chance of same GCI
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlDeleteDeps2059+"\n"+
		sqlDeleteFV2059+"\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// Wait for event processing
	time.Sleep(5 * time.Second)

	// Cache should have a valid entry (FV was re-inserted).
	// With mergeEvents(true), DELETE+INSERT → UPDATE → not subscribed → NO event.
	// So the cache would retain the OLD entry (still IS_VALID), and status would be 200.
	// With mergeEvents(false) or proper handling, we'd see BOTH events processed.
	status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
		fvVersionSimple, "id1", "1")
	if status == http.StatusOK {
		t.Log("RESULT: DELETE+INSERT same batch → cache shows 200 → could be old entry or properly reloaded")
	} else {
		t.Logf("RESULT: DELETE+INSERT same batch → cache shows %d → eviction happened but reload failed/not yet", status)
	}
	// Don't fail — this is diagnostic
}
