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

package index_scan

import (
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// Example 1: Simple comparison filter - "val_1" >= "1"
func Test_SimpleComparison(t *testing.T) {
	database := testdbs.DB029
	table := "tiny_tbl" // using tiny table as both rest and mysql will read the entire table.

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "val_1",
			Cond:   "GE",
			Value:  1,
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

// Example 2: Simple ISNOTNULL filter
func Test_IsNotNull(t *testing.T) {
	database := testdbs.DB029
	table := "tiny_tbl" // using tiny table as both rest and mysql will read the entire table.

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op:     "ISNOTNULL",
			Column: "content",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

// Example 3: Complex filter from design doc
// (content IS NOT NULL AND pk > 2) AND (val_1 <= 30 OR val_2 > 500)
// using big_tbl as in this test we are using asc order and a limit
func Test_ComplexFilterWithIndex(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl"

	col1 := "pk"
	col2 := "val_1"
	col3 := "val_2"
	col4 := "content"

	readColumns := []api.ReadColumn{
		{Column: &col1},
		{Column: &col2},
		{Column: &col3},
		{Column: &col4},
	}

	query := api.IndexScanQuery{
		Limit:       10,
		ReadColumns: &readColumns,
		Filters: &api.ScanFilter{
			Op: "AND",
			Args: []*api.ScanFilter{
				{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNOTNULL",
							Column: "content",
						},
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "GT",
							Value:  2,
						},
					},
				},
				{
					Op: "OR",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "val_1",
							Cond:   "LE",
							Value:  30,
						},
						{
							Op:     "CMP",
							Column: "val_2",
							Cond:   "GT",
							Value:  500,
						},
					},
				},
			},
		},
		Index: &api.IndexScan{
			Name:       "idx_val",
			KeyColumns: []string{"val_1", "val_2"},
			Ranges: []api.RangeScan{
				{
					Lower: api.BoundedScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundedScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MUST_MATCH)
}

// Example 3: Complex filter from design doc
// (content IS NOT NULL AND pk > 2) AND (val_1 <= 30 OR val_2 > 500)
func Test_ComplexFilterWithOutIndex(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl"

	col1 := "pk"
	col2 := "val_1"
	col3 := "val_2"
	col4 := "content"

	readColumns := []api.ReadColumn{
		{Column: &col1},
		{Column: &col2},
		{Column: &col3},
		{Column: &col4},
	}

	query := api.IndexScanQuery{
		Limit:       math.MaxInt,
		ReadColumns: &readColumns,
		Filters: &api.ScanFilter{
			Op: "AND",
			Args: []*api.ScanFilter{
				{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNOTNULL",
							Column: "content",
						},
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "GT",
							Value:  2,
						},
					},
				},
				{
					Op: "OR",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "val_1",
							Cond:   "LE",
							Value:  30,
						},
						{
							Op:     "CMP",
							Column: "val_2",
							Cond:   "GT",
							Value:  500,
						},
					},
				},
			},
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

// TODO this test some times fail
// Example 4: AND operation - ("val_1" >= "1") AND ("val_2" >= "1")
func Test_AndOperation(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl"

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op: "AND",
			Args: []*api.ScanFilter{
				{
					Op:     "CMP",
					Column: "val_1",
					Cond:   "GE",
					Value:  1,
				},
				{
					Op:     "CMP",
					Column: "val_2",
					Cond:   "GE",
					Value:  1,
				},
			},
		},
		Index: &api.IndexScan{
			Name:       "idx_val",
			KeyColumns: []string{"val_1", "val_2"},
			Ranges: []api.RangeScan{
				{
					Lower: api.BoundedScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundedScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MUST_MATCH)
}

// Example 5: Index scan without filters
func Test_IndexScanOnly(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl"

	query := api.IndexScanQuery{
		Limit: math.MaxInt,
		Index: &api.IndexScan{
			Name:       "idx_val",
			KeyColumns: []string{"val_1", "val_2"},
			Ranges: []api.RangeScan{
				{
					Lower: api.BoundedScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundedScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MUST_MATCH)
}

// Example 6: Filter without index (table scan)
func Test_TableScanWithFilter(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl"

	query := api.IndexScanQuery{
		Limit: math.MaxInt,
		Filters: &api.ScanFilter{
			Op: "OR",
			Args: []*api.ScanFilter{
				{
					Op:     "CMP",
					Column: "val_1",
					Cond:   "LT",
					Value:  10,
				},
				{
					Op:     "CMP",
					Column: "val_2",
					Cond:   "GT",
					Value:  1000,
				},
			},
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

// Simple comparison filter on pk col - "pk" >= "1" and "pk" <= "10"
func Test_SimpleComparisonOnPkCol(t *testing.T) {
	database := testdbs.DB029
	table := "big_tbl2" // using tiny table as both rest and mysql will read the entire table.

	query := api.IndexScanQuery{
		Limit: 100,
		Filters: &api.ScanFilter{
			Op: "AND",
			Args: []*api.ScanFilter{
				{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GE",
					Value:  0,
				},
				{
					Op:     "CMP",
					Column: "pk",
					Cond:   "LT",
					Value:  10,
				},
			},
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

// No operations are running when schema is changed.
func Test_SchemaVersionChangeNonConcurrent(t *testing.T) {
	// Reset database at start
	err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
	if err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}

	defer func() { // reset database at end
		err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
		if err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	database := testdbs.DB025
	table := "table_2"

	query := api.IndexScanQuery{
		Limit: 100,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "id0",
			Cond:   "EQ",
			Value:  1,
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	loop := 256
	for i := 0; i < loop; i++ {
		restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
		if err != nil {
			t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
		}
		CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
	}

	// drop and recreate the database. this will change schema version
	err = testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme)
	if err != nil {
		t.Fatalf("failed to re-create tables. Error: %v", err)
	}

	mysqlRows, mysqlCols, err = ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	for i := 0; i < loop; i++ {
		restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
		if err != nil {
			t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
		}
		CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
	}
}

// Test_SchemaVersionChangeConcurrent runs scan operations concurrently while schema is being changed.
// This tests the REST server's ability to handle schema version mismatch errors (error 241).
func Test_SchemaVersionChangeConcurrent(t *testing.T) {
	// Reset database at start
	err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
	if err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}

	defer func() { // reset database at end
		err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
		if err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	database := testdbs.DB025
	table := "table_2"

	query := api.IndexScanQuery{
		Limit: 100,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "id0",
			Cond:   "EQ",
			Value:  1,
		},
	}

	// Start worker goroutines making continuous scan requests
	numWorkers := 1
	var stop atomic.Bool
	stop.Store(false)
	done := make(chan int, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			count := 0
			defer func() {
				done <- count
			}()
			for !stop.Load() {
				restRows, _, _, _ := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
				if len(restRows) != 1 {
					stop.Store(true)
					t.Errorf("worker %d: wrong data read. Expecting one row to read. Got: %d rows", workerID, len(restRows))
				}
				count++
			}
		}(i)
	}

	// Let requests run to cache schema on all REST server threads
	time.Sleep(2 * time.Second)

	// Change schema WHILE requests are still running
	// This should trigger schema version mismatch errors on some requests
	t.Log("Changing schema...")
	err = testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme)
	if err != nil {
		t.Fatalf("failed to update schema. Error: %v", err)
	}
	t.Log("Schema changed")

	// Continue running requests for a bit after schema change
	time.Sleep(2 * time.Second)

	// Stop workers
	stop.Store(true)

	// Wait for all workers and count total operations
	totalOps := 0
	for i := 0; i < numWorkers; i++ {
		totalOps += <-done
	}
	t.Logf("Total operations completed: %d", totalOps)

	// Verify final state - requests should work after schema change
	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed after schema change: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, NO_ERROR_MSG, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed after schema change: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}
