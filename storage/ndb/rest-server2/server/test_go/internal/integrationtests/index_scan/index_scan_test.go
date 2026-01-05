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
	"testing"

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
