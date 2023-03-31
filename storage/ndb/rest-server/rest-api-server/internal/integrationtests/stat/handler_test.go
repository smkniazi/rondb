/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package stat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestStat(t *testing.T) {

	ch := make(chan int)

	numOps := uint32(50)
	expectedAllocations := numOps * 2

	conf := config.GetAll()

	preAllocatedBuffers := conf.Internal.PreAllocatedBuffers
	if preAllocatedBuffers > numOps {
		expectedAllocations = preAllocatedBuffers
	}

	for i := uint32(0); i < numOps; i++ {
		go performPkOp(t, ch)
	}
	for i := uint32(0); i < numOps; i++ {
		<-ch
	}

	// get stats
	statsHttp := getStatsHttp(t)
	compare(t, statsHttp, int64(expectedAllocations), int64(numOps))

	statsGRPC := getStatsGRPC(t)
	compare(t, statsGRPC, int64(expectedAllocations), int64(numOps))

	fmt.Printf("Test completed\n")
}

func compare(t *testing.T, stats *api.StatResponse, expectedAllocations int64, numOps int64) {
	if stats.MemoryStats.AllocationsCount != expectedAllocations ||
		stats.MemoryStats.BuffersCount != expectedAllocations ||
		stats.MemoryStats.FreeBuffers != expectedAllocations {
		t.Fatalf("Native buffer stats do not match Got: %v", stats)
	}

	// Number of NDB objects created must be equal to number of NDB
	// objects freed
	if stats.RonDBStats.NdbObjectsTotalCount != stats.RonDBStats.NdbObjectsFreeCount {
		t.Fatalf("RonDB stats do not match. %#v", stats.RonDBStats)
	}
}

func performPkOp(t *testing.T, ch chan int) {

	db := testdbs.DB004
	table := "int_table"
	ops := api.BatchOperationTestInfo{ // bigger batch of numbers table
		HttpCode: []int{http.StatusOK, http.StatusNotFound},
		Operations: []api.BatchSubOperationTestInfo{
			{
				SubOperation: api.BatchSubOp{
					Method:      &[]string{config.PK_HTTP_VERB}[0],
					RelativeURL: &[]string{string(db + "/" + table + "/" + config.PK_DB_OPERATION)}[0],
					Body: &api.PKReadBody{
						Filters:     integrationtests.NewFiltersKVs("id0", 0, "id1", 0),
						ReadColumns: integrationtests.NewReadColumns("col", 2),
						OperationID: integrationtests.NewOperationID(64),
					},
				},
				Table:    table,
				DB:       db,
				HttpCode: http.StatusOK,
				RespKVs:  []interface{}{"col0", "col1"},
			},
		},
		ErrMsgContains: "",
	}

	defer func() {
		ch <- 0
	}()

	integrationtests.BatchGRPCTest(t, ops, false, true)

}

func getStatsHttp(t *testing.T) *api.StatResponse {
	body := ""
	url := testutils.NewStatURL()
	_, respBody := integrationtests.SendHttpRequest(t, config.STAT_HTTP_VERB, url, string(body),
		"", http.StatusOK)

	var stats api.StatResponse
	err := json.Unmarshal([]byte(respBody), &stats)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return &stats
}

func getStatsGRPC(t *testing.T) *api.StatResponse {
	stats := sendGRPCStatRequest(t)
	return stats
}

func sendGRPCStatRequest(t *testing.T) *api.StatResponse {
	// Create gRPC client
	client := api.NewRonDBRESTClient(integrationtests.GetGRPCConnction())

	// Create Request
	statRequest := api.StatRequest{}

	reqProto := api.ConvertStatRequest(&statRequest)

	expectedStatus := http.StatusOK
	respCode := 200
	var errStr string
	respProto, err := client.Stat(context.Background(), reqProto)
	if err != nil {
		respCode = integrationtests.GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Received unexpected status; Expected: %d, Got: %d; Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	return api.ConvertStatResponseProto(respProto)
}
