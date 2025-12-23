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
	"testing"

	"hopsworks.ai/rdrs2/pkg/api"
)

// Example 1: Simple comparison filter - "val_1" >= "1"
func Test_SimpleComparison(t *testing.T) {
	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.FilterScan{
			Op:     "CMP",
			Column: "val_1",
			Cond:   "GE",
			Value:  1,
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 2: Simple ISNOTNULL filter
func Test_IsNotNull(t *testing.T) {
	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.FilterScan{
			Op:     "ISNOTNULL",
			Column: "content",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 3: Complex filter from design doc
// (content IS NOT NULL AND pk > 2) AND (val_1 <= 30 OR val_2 > 500)
func Test_ComplexFilterWithIndex(t *testing.T) {
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
		Filters: &api.FilterScan{
			Op: "AND",
			Args: []*api.FilterScan{
				{
					Op: "AND",
					Args: []*api.FilterScan{
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
					Args: []*api.FilterScan{
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
					Lower: api.BoundScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 3: Complex filter from design doc
// (content IS NOT NULL AND pk > 2) AND (val_1 <= 30 OR val_2 > 500)
func Test_ComplexFilterWithOutIndex(t *testing.T) {
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
		Filters: &api.FilterScan{
			Op: "AND",
			Args: []*api.FilterScan{
				{
					Op: "AND",
					Args: []*api.FilterScan{
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
					Args: []*api.FilterScan{
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 4: AND operation - ("val_1" >= "1") AND ("val_2" >= "1")
func Test_AndOperation(t *testing.T) {
	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.FilterScan{
			Op: "AND",
			Args: []*api.FilterScan{
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
					Lower: api.BoundScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 5: Index scan without filters
func Test_IndexScanOnly(t *testing.T) {
	query := api.IndexScanQuery{
		Limit: 10,
		Index: &api.IndexScan{
			Name:       "idx_val",
			KeyColumns: []string{"val_1", "val_2"},
			Ranges: []api.RangeScan{
				{
					Lower: api.BoundScan{
						Values:    []any{0, 0},
						Inclusive: true,
					},
					Upper: api.BoundScan{
						Values:    []any{1000, 1000},
						Inclusive: false,
					},
				},
			},
			Order: "asc",
		},
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}

// Example 6: Filter without index (table scan)
func Test_TableScanWithFilter(t *testing.T) {
	query := api.IndexScanQuery{
		Limit: 50,
		Filters: &api.FilterScan{
			Op: "OR",
			Args: []*api.FilterScan{
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

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	restRows, restCols, respCode, err := ExecuteUsingRESTServer(t, &query)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	if respCode != 200 {
		t.Fatalf("Expected response code 200, got %d", respCode)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols)
}
