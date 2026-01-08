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
	"encoding/base64"
	"encoding/binary"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/common"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

func Test_NotFound(t *testing.T) {
	database := testdbs.DB029
	table := "tiny_tbl" // using tiny table as both rest and mysql will read the entire table.

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "val_1",
			Cond:   "EQ",
			Value:  -404,
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

func Test_Projection(t *testing.T) {
	database := testdbs.DB029
	table := "tiny_tbl" // using tiny table as both rest and mysql will read the entire table.

	col3 := "val_2"
	col4 := "content"

	readColumns := []api.ReadColumn{
		{Column: &col3},
		{Column: &col4},
	}

	query := api.IndexScanQuery{
		Limit:       10,
		ReadColumns: &readColumns,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "val_1",
			Cond:   "GT",
			Value:  0,
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	loop := 256
	for i := 0; i < loop; i++ {
		restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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

	mysqlRows, mysqlCols, err = ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	for i := 0; i < loop; i++ {
		restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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
				restRows, _, _, _ := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
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
	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, database, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed after schema change: %v", err)
	}

	restRows, restCols, _, err := ExecuteUsingRESTServer(t, database, table, &query, EMPTY_STRING, http.StatusOK)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed after schema change: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}

func TestDataTypesInt(t *testing.T) {
	testDB := testdbs.DB004
	testTable := "int_table"

	tests := map[string]api.IndexTestInfo{
		"404": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  100,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  100,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusNotFound,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"max_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  2147483647,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  4294967295,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"min_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -2147483648,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING, // TODO FIX ME
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{ // bigger than the range
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  2147483648,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  4294967295,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING, // TODO FIX ME
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningSmallerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{ // bigger than the range
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -2147483649,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING, // TODO FIX ME
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNULL",
							Column: "id0",
						},
						{
							Op:     "ISNULL",
							Column: "id1",
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInCols": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesBigInt(t *testing.T) {
	testDB := testdbs.DB005
	testTable := "bigint_table"

	tests := map[string]api.IndexTestInfo{
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"max_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  int64(9223372036854775807),
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  uint64(18446744073709551615),
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"min_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  int64(-9223372036854775808),
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -1, // id1 is unsigned
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  int64(9223372036854775807),
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  "18446744073709551616", // 18446744073709551615+1
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningSmallerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  "-9223372036854775809", // -9223372036854775808-1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNULL",
							Column: "id0",
						},
						{
							Op:     "ISNULL",
							Column: "id1",
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInCols": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesTinyInt(t *testing.T) {
	testDB := testdbs.DB006
	testTable := "tinyint_table"

	tests := map[string]api.IndexTestInfo{
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"max_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  127,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  255,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"min_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -128,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -1, // id1 is unsigned
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  127,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  256, // 255+1
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningSmallerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -129, // -128-1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNULL",
							Column: "id0",
						},
						{
							Op:     "ISNULL",
							Column: "id1",
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInCols": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesSmallInt(t *testing.T) {
	testDB := testdbs.DB007
	testTable := "smallint_table"

	tests := map[string]api.IndexTestInfo{
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"max_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  32767,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  65535,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"min_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -32768,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -1, // id1 is unsigned
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  32768, // 32767+1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  256,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningSmallerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -32769, // -32768-1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNULL",
							Column: "id0",
						},
						{
							Op:     "ISNULL",
							Column: "id1",
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInCols": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesMediumInt(t *testing.T) {
	testDB := testdbs.DB008
	testTable := "mediumint_table"

	tests := map[string]api.IndexTestInfo{
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"max_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  8388607,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  16777215,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"min_pk_values": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -8388608,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -1, // id1 is unsigned
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  8388608, // 8388607+1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  256,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningSmallerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -8388609, // -8388608-1
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  0,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "ISNULL",
							Column: "id0",
						},
						{
							Op:     "ISNULL",
							Column: "id1",
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullValsInCols": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesFloat(t *testing.T) {
	testDB := testdbs.DB009

	tests := map[string]api.IndexTestInfo{
		"floatPK": { // NDB does not support float PKs
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  0,
				},
			},
			Table:            "float_table2",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_017(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  0,
				},
			},
			Table:            "float_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1",
				},
			},
			Table:            "float_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  2,
				},
			},
			Table:            "float_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesDouble(t *testing.T) {
	testDB := testdbs.DB010

	tests := map[string]api.IndexTestInfo{
		"doublePK": { // NDB does not support double PKs
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  0,
				},
			},
			Table:            "double_table2",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_017(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  0,
				},
			},
			Table:            "double_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:            "double_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  2,
				},
			},
			Table:            "double_table1",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesDecimal(t *testing.T) {
	testDB := testdbs.DB011
	testTable := "decimal_table"

	tests := map[string]api.IndexTestInfo{
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -12345.12345,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  12345.12345,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nullVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -67890.12345,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  67890.12345,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assignNegativeValToUnsignedCol": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -12345.12345,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  -12345.12345, // id1 is unsigned
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_015(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"assigningBiggerVals": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "id0",
							Cond:   "EQ",
							Value:  -12345.12345,
						},
						{
							Op:     "CMP",
							Column: "id1",
							Cond:   "EQ",
							Value:  123456789.12345, // value too large
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_015(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesDatetimeColumn(t *testing.T) {
	testDB := testdbs.DB020

	tests := map[string]api.IndexTestInfo{
		"validpk1_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11",
				},
			},
			Table:            "date_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11.123",
				},
			},
			Table:            "date_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11.123456",
				},
			},
			Table:            "date_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre0": { // nanoseconds should be ignored
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11.123123",
				},
			},
			Table:            "date_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11.123000",
				},
			},
			Table:            "date_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre6": { // -ve sign should be ignored
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 -11:11:11.123456",
				},
			},
			Table:            "date_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-12 11:11:11",
				},
			},
			Table:            "date_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-12 11:11:11.123",
				},
			},
			Table:            "date_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-12 11:11:11.123456",
				},
			},
			Table:            "date_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"wrongdate_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-13-11 11:11:11", // invalid month
				},
			},
			Table:            "date_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesTimeColumn(t *testing.T) {
	testDB := testdbs.DB021

	tests := map[string]api.IndexTestInfo{
		"validpk1_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:11:11",
				},
			},
			Table:            "time_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:11:11.123",
				},
			},
			Table:            "time_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:11:11.123456",
				},
			},
			Table:            "time_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre0": { // nanoseconds should be ignored
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:11:11.123123",
				},
			},
			Table:            "time_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:11:11.123000",
				},
			},
			Table:            "time_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "12:11:11",
				},
			},
			Table:            "time_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "12:11:11.123",
				},
			},
			Table:            "time_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "12:11:11.123456",
				},
			},
			Table:            "time_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"wrongtime_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "11:61:11", // invalid minutes
				},
			},
			Table:            "time_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesTimestampColumn(t *testing.T) {
	testDB := testdbs.DB022

	tests := map[string]api.IndexTestInfo{
		"badts_1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:11:11",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"badts_2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1970-01-01 00:00:00",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"badts_3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2038-01-19 03:14:08",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 11:11:11",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 11:11:11.123",
				},
			},
			Table:            "ts_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk1_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 11:11:11.123456",
				},
			},
			Table:            "ts_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre0": { // nanoseconds should be ignored
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 11:11:11.123123",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 11:11:11.123000",
				},
			},
			Table:            "ts_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2_pre6": { // -ve sign should be ignored
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-11 -11:11:11.123456",
				},
			},
			Table:            "ts_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-12 11:11:11",
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre3": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-12 11:11:11.123",
				},
			},
			Table:            "ts_table3",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest_pre6": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-11-12 11:11:11.123456",
				},
			},
			Table:            "ts_table6",
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"wrongdate_pre0": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022-13-11 11:11:11", // invalid month
				},
			},
			Table:            "ts_table0",
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_027(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesYearColumn(t *testing.T) {
	// Year 1901-2155 (1 byte)
	testDB := testdbs.DB023
	testTable := "year_table"

	tests := map[string]api.IndexTestInfo{
		"simple1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2022",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"notfound1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1901",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"notfound2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2155",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2023",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"baddate1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1900", // below valid range
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_015(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"baddate2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2156", // above valid range
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_015(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesBitColumn(t *testing.T) {
	testDB := testdbs.DB024
	testTable := "bit_table"

	tests := map[string]api.IndexTestInfo{
		"simple1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("1", true, 100, true),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("2", true, 100, true),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"null": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("3", true, 100, true),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_NEEDS_BINARY_ENCODING)
}

func TestDataTypesDateColumn(t *testing.T) {
	testDB := testdbs.DB019
	testTable := "date_table"

	tests := map[string]api.IndexTestInfo{
		"validpk1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"validpk2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 00:00:00",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"invalidpk": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 11:00:00",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_008(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"invalidpk2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-11 00:00:00.123123",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_008(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-11-12",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"error": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1111-13-11", // invalid month
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        common.ERROR_027(),
			RowsOrder:           ROWS_ORDER_MUST_MATCH,
			SkipMySQLValidation: true,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesDefaultValues(t *testing.T) {
	testDB := testdbs.DB028
	testTable := "table_1"

	tests := map[string]api.IndexTestInfo{
		"test1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id",
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestLargePks(t *testing.T) {
	testDB := testdbs.DB026
	testTable := "table_1"

	pkData := make([]byte, 3070)
	for i := 0; i < 3070; i++ {
		pkData[i] = 0x41
	}
	pkDataEncoded := base64.StdEncoding.EncodeToString(pkData)

	tests := map[string]api.IndexTestInfo{
		"largePk": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id",
					Cond:   "EQ",
					Value:  pkDataEncoded,
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_NEEDS_BINARY_ENCODING)
}

func TestLargeBase64Pk(t *testing.T) {
	testDB := testdbs.DB026
	testTable := "table_2"

	someNumber := 7
	pkTotalLength := 3000 // id0 is 3000 bytes
	id0 := make([]byte, pkTotalLength-8)
	actualData := make([]byte, 8)
	binary.LittleEndian.PutUint64(actualData, uint64(someNumber))
	allData := append(id0, actualData...)
	pkDataEncoded := base64.StdEncoding.EncodeToString(allData)

	tests := map[string]api.IndexTestInfo{
		"largeBase64Pk": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id",
					Cond:   "EQ",
					Value:  pkDataEncoded,
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_NEEDS_BINARY_ENCODING)
}

func TestLargeColumn(t *testing.T) {
	testDB := testdbs.DB027
	testTable := "table_1"

	decoded := []byte("1")
	pkDataEncoded := base64.StdEncoding.EncodeToString(decoded)

	tests := map[string]api.IndexTestInfo{
		"ok": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id",
					Cond:   "EQ",
					Value:  pkDataEncoded,
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"notBase64String": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id",
					Cond:   "EQ",
					Value:  "1",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_NEEDS_BINARY_ENCODING)
}

func TestDataTypesText(t *testing.T) {
	testDB := testdbs.DB013
	testTable := "text_table"

	tests := map[string]api.IndexTestInfo{
		"notfound": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "-1",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"null": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "2",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "1",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple2": { // all characters needs to be escaped. ascii char that needs escaping
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "3",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple3": { // all characters needs to be escaped. non printable ascii char for example 0x17
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "4",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple4": { // all characters needs to be escaped. non printable unicode
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  "5",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesBlob(t *testing.T) {
	testDB := testdbs.DB013
	testTable := "blob_table"

	tests := map[string]api.IndexTestInfo{
		"notfound": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("-1", true, 255, false),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"null": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("2", true, 255, false),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("1", true, 255, false),
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_NEEDS_BINARY_ENCODING)
}

func TestDataTypesChar(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB012, false, 100, DATA_NEEDS_BINARY_ENCODING)
}

func TestDataTypesVarchar(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB014, false, 50, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesLongVarchar(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB015, false, 256, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesBinary(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB016, true, 100, DATA_NEEDS_BINARY_ENCODING)
}

func TestDataTypesVarbinary(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB017, true, 100, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestDataTypesLongVarbinary(t *testing.T) {
	arrayColumnTest(t, "table1", testdbs.DB018, true, 256, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// arrayColumnTest is a helper function for testing char/varchar/binary column types
func arrayColumnTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {
	tests := map[string]api.IndexTestInfo{
		"notfound1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("-1", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"badRequest1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue(*testclient.NewOperationID(colWidth*4 + 1), isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusBadRequest,
			BodyContains:     common.ERROR_008(),
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple1": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("1", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple2": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("2", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple3": { // new line char in string
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("3", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple4": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("4", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"simple5": { // unicode pk
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("这是一个测验", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"nulltest": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("5", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"escapedChars": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("6", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
		"quotedPK": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 100,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "id0",
					Cond:   "EQ",
					Value:  testclient.EncodePkValue("\"7\"", isBinary, colWidth, padding),
				},
			},
			Table:            table,
			DB:               database,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, isBinary)
}

