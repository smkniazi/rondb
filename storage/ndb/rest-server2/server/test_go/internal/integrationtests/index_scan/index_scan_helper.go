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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
	"hopsworks.ai/rdrs2/version"
)

// ConvertToSQL converts an IndexScanQuery to a SQL SELECT statement
func ConvertToSQL(database string, table string, query *api.IndexScanQuery) (string, error) {
	var sqlBuilder strings.Builder

	sqlBuilder.WriteString("SELECT ")
	if query.ReadColumns != nil && len(*query.ReadColumns) > 0 {
		columns := make([]string, 0, len(*query.ReadColumns))
		for _, col := range *query.ReadColumns {
			if col.Column != nil {
				columns = append(columns, *col.Column)
			}
		}
		sqlBuilder.WriteString(strings.Join(columns, ", "))
	} else {
		sqlBuilder.WriteString("*")
	}

	sqlBuilder.WriteString(fmt.Sprintf(" FROM %s.%s", database, table))

	if query.Filters != nil {
		whereClause, err := convertFilterToSQL(query.Filters)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(whereClause)
	}

	if query.Index != nil && query.Index.Order != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(strings.Join(query.Index.KeyColumns, ", "))
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(strings.ToUpper(query.Index.Order))
	}

	if query.Limit > 0 {
		sqlBuilder.WriteString(fmt.Sprintf(" LIMIT %d", query.Limit))
	}

	sqlBuilder.WriteString(";")
	return sqlBuilder.String(), nil
}

// convertFilterToSQL recursively converts FilterScan to SQL WHERE clause
func convertFilterToSQL(filter *api.FilterScan) (string, error) {
	switch filter.Op {
	case "AND", "OR", "NAND", "NOR":
		if filter.Args == nil || len(filter.Args) == 0 {
			return "", fmt.Errorf("logical operator %s requires args", filter.Op)
		}

		subClauses := make([]string, 0, len(filter.Args))
		for _, arg := range filter.Args {
			subClause, err := convertFilterToSQL(arg)
			if err != nil {
				return "", err
			}
			subClauses = append(subClauses, fmt.Sprintf("(%s)", subClause))
		}

		var operator string
		switch filter.Op {
		case "AND":
			operator = " AND "
		case "OR":
			operator = " OR "
		case "NAND":
			return fmt.Sprintf("NOT (%s)", strings.Join(subClauses, " AND ")), nil
		case "NOR":
			return fmt.Sprintf("NOT (%s)", strings.Join(subClauses, " OR ")), nil
		}
		return strings.Join(subClauses, operator), nil

	case "CMP":
		if filter.Column == "" {
			return "", fmt.Errorf("CMP operator requires column")
		}
		if filter.Cond == "" {
			return "", fmt.Errorf("CMP operator requires cond")
		}

		var sqlOperator string
		switch filter.Cond {
		case "EQ":
			sqlOperator = "="
		case "NE":
			sqlOperator = "!="
		case "GT":
			sqlOperator = ">"
		case "GE":
			sqlOperator = ">="
		case "LT":
			sqlOperator = "<"
		case "LE":
			sqlOperator = "<="
		default:
			return "", fmt.Errorf("unknown condition: %s", filter.Cond)
		}

		value := formatValue(filter.Value)
		return fmt.Sprintf("%s %s %s", filter.Column, sqlOperator, value), nil

	case "ISNOTNULL":
		if filter.Column == "" {
			return "", fmt.Errorf("ISNOTNULL operator requires column")
		}
		return fmt.Sprintf("%s IS NOT NULL", filter.Column), nil

	case "ISNULL":
		if filter.Column == "" {
			return "", fmt.Errorf("ISNULL operator requires column")
		}
		return fmt.Sprintf("%s IS NULL", filter.Column), nil

	default:
		return "", fmt.Errorf("unknown operator: %s", filter.Op)
	}
}

// formatValue formats a value for SQL
func formatValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// Escape single quotes in strings
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		// For other types, convert to string and quote
		return fmt.Sprintf("'%v'", v)
	}
}

// GetSampleData executes a SQL query and returns the result rows
// Returns: rows ([][]interface{}), column names ([]string), column types ([]string), error
func GetSampleData(db *sql.DB, sqlQuery string) ([][]interface{}, []string, []string, error) {
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get column types: %w", err)
	}

	colTypeNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		colTypeNames[i] = ct.DatabaseTypeName()
	}

	// Fetch all rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create slice to hold column values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row into value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert sql.RawBytes to appropriate types
		row := make([]interface{}, len(columns))
		for i, val := range values {
			if val == nil {
				row[i] = nil
			} else {
				// Convert []byte to string or keep as-is based on type
				switch v := val.(type) {
				case []byte:
					row[i] = string(v)
				default:
					row[i] = v
				}
			}
		}

		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if len(resultRows) == 0 {
		return nil, columns, colTypeNames, fmt.Errorf("no data returned from query")
	}

	return resultRows, columns, colTypeNames, nil
}

// GetSampleDataWithQuery executes an IndexScanQuery and returns the result rows
func GetSampleDataWithQuery(db *sql.DB, database string, table string, query *api.IndexScanQuery) ([][]interface{}, []string, []string, error) {
	sqlQuery, err := ConvertToSQL(database, table, query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to convert query to SQL: %w", err)
	}

	return GetSampleData(db, sqlQuery)
}

// ExecuteUsingMySQLServer is a helper function to execute query and print results for testing
// Returns: rows ([][]interface{}), column names ([]string), error
func ExecuteUsingMySQLServer(t *testing.T, query *api.IndexScanQuery) ([][]interface{}, []string, error) {
	jsonBytes, err := json.MarshalIndent(query, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal query: %w", err)
	}
	log.Debugf("JSON:\n%s\n", string(jsonBytes))

	sql, err := ConvertToSQL(testdbs.DB029, "tbl", query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert to SQL: %w", err)
	}
	log.Infof("Request SQL:\n%s\n", sql)

	db, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create DB connection: %w", err)
	}
	defer db.Close()

	rows, columns, colTypes, err := GetSampleData(db, sql)
	if err != nil {
		log.Infof("Query returned no data or error: %v", err)
		return nil, nil, err
	}

	log.Infof("Columns: %v (Types: %v)", columns, colTypes)
	log.Infof("Sample data (%d rows):", len(rows))
	for i, row := range rows {
		log.Infof("  Row %d: %v", i, row)
	}
	log.Infof("\n" + strings.Repeat("=", 80) + "\n")

	return rows, columns, nil
}

// NewIndexScanURL creates a URL for the index scan endpoint
func NewIndexScanURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s/%s/scan",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		db,
		table,
	)
	if conf.Security.TLS.EnableTLS {
		url = fmt.Sprintf("https://%s", url)
	} else {
		url = fmt.Sprintf("http://%s", url)
	}
	return url
}

// ExecuteUsingRESTServer is a helper function to execute query via REST endpoint and print results
// Returns: rows ([][]any), column names ([]string), response code (int), error
func ExecuteUsingRESTServer(t *testing.T, query *api.IndexScanQuery) ([][]any, []string, int, error) {
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to marshal query: %w", err)
	}

	log.Infof("JSON Request:\n%s\n", string(jsonBytes))

	url := NewIndexScanURL(testdbs.DB029, "tbl")

	respCode, respBody := testclient.SendHttpRequest(
		t,
		http.MethodPost,
		url,
		string(jsonBytes),
		"",
		http.StatusOK,
	)

	log.Infof("Response Code: %d. Body: %s \n", respCode, string(respBody))

	var scanResp api.IndexScanResponse
	err = json.Unmarshal(respBody, &scanResp)
	if err != nil {
		return nil, nil, respCode, fmt.Errorf("failed to unmarshal response body: %w", err)
	}

	if len(scanResp.Data) == 0 {
		return [][]any{}, []string{}, respCode, nil
	}

	var columnNames []string
	for colName := range scanResp.Data[0] {
		columnNames = append(columnNames, colName)
	}

	rows := make([][]any, len(scanResp.Data))
	for i, rowMap := range scanResp.Data {
		row := make([]any, len(columnNames))
		for j, colName := range columnNames {
			row[j] = rowMap[colName]
		}
		rows[i] = row
	}

	log.Infof("Parsed data (%d rows):", len(rows))
	log.Infof("Column order: %v", columnNames)
	for i, row := range rows {
		log.Infof("  Row %d: %v", i, row)
	}
	log.Infof("\n" + strings.Repeat("=", 80) + "\n")

	return rows, columnNames, respCode, nil
}

// CompareResults compares MySQL and REST server results using string-based comparison
func CompareResults(t *testing.T, mysqlRows [][]interface{}, mysqlCols []string,
	restRows [][]any, restCols []string) {

	if len(mysqlCols) != len(restCols) {
		t.Fatalf("Column count mismatch: MySQL=%d, REST=%d", len(mysqlCols), len(restCols))
	}

	for i := range mysqlCols {
		if mysqlCols[i] != restCols[i] {
			t.Fatalf("Column name mismatch at index %d: MySQL=%s, REST=%s",
				i, mysqlCols[i], restCols[i])
		}
	}

	if len(mysqlRows) != len(restRows) {
		t.Fatalf("Row count mismatch: MySQL=%d, REST=%d", len(mysqlRows), len(restRows))
	}

	for i := range mysqlRows {
		for j := range mysqlRows[i] {
			mysqlVal := fmt.Sprintf("%v", mysqlRows[i][j])
			restVal := fmt.Sprintf("%v", restRows[i][j])

			if mysqlVal != restVal {
				t.Errorf("Value mismatch at row %d, col %d (%s): MySQL=%s, REST=%s",
					i, j, mysqlCols[j], mysqlVal, restVal)
			}
		}
	}
}
