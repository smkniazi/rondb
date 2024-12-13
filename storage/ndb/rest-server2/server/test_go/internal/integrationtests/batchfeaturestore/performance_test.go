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

package batchfeaturestore

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"hopsworks.ai/rdrs2/internal/config"
	fshelper "hopsworks.ai/rdrs2/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

type GetRandomRequest func(b *testing.B, batchSize int) (string, string, string)

func BenchmarkBatchSimple(b *testing.B) {
	var batchSize = 10
	fn := func(b *testing.B, batchSize int) (verb, url, body string) {
		var fsName = testdbs.FSDB001
		var fvName = "sample_1"
		var fvVersion = 1

		verb = config.FEATURE_STORE_HTTP_VERB
		url = testutils.NewBatchFeatureStoreURL()
		rows, pks, cols, err := fshelper.GetSampleDataN(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion), batchSize)
		if err != nil {
			b.Fatalf("Cannot get sample data with error %s ", err)
			return
		}

		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&rows, &pks, &cols),
			nil,
			nil)

		strBytes, err := json.MarshalIndent(fsReq, "", "")
		if err != nil {
			b.Fatalf("Failed to marshal FeatureStoreRequest. Error: %v", err)
			return
		}
		body = string(strBytes)

		return
	}

	WrapperBenchmark(b, batchSize, fn)
}

func BenchmarkBatchJoin(b *testing.B) {
	var batchSize = 10
	fn := func(b *testing.B, batchSize int) (verb, url, body string) {
		var fsName = testdbs.FSDB001
		var fvName = "sample_1n2"
		var fvVersion = 1

		verb = config.FEATURE_STORE_HTTP_VERB
		url = testutils.NewBatchFeatureStoreURL()
		rows, pks, cols, err := fshelper.GetSampleDataN(fsName, "sample_1_1", batchSize)
		if err != nil {
			b.Fatalf("Cannot get sample data with error %s ", err)
			return
		}

		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&rows, &pks, &cols),
			nil,
			nil)

		strBytes, err := json.MarshalIndent(fsReq, "", "")
		if err != nil {
			b.Fatalf("Failed to marshal FeatureStoreRequest. Error: %v", err)
			return
		}
		body = string(strBytes)

		return
	}

	WrapperBenchmark(b, batchSize, fn)
}

func BenchmarkBatchComplex512(b *testing.B) {

	// Uncomment this if you want to generate more random data in the test table
	// generateRandomComplexData(10/*number of rows*/, 512/*number of cols*/)

	var batchSize = 10
	fn := func(b *testing.B, batchSize int) (verb, url, body string) {
		var fsName = testdbs.FSDB002
		var fvName = "sample_complex_type_512"
		var fvVersion = 1

		verb = config.FEATURE_STORE_HTTP_VERB
		url = testutils.NewBatchFeatureStoreURL()
		rows, pks, cols, err := fshelper.GetSampleDataN(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion), batchSize)
		if err != nil {
			b.Fatalf("Cannot get sample data with error %s ", err)
			return
		}

		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&rows, &pks, &cols),
			nil,
			nil)

		strBytes, err := json.MarshalIndent(fsReq, "", "")
		if err != nil {
			b.Fatalf("Failed to marshal FeatureStoreRequest. Error: %v", err)
			return
		}
		body = string(strBytes)

		return
	}

	WrapperBenchmark(b, batchSize, fn)
}

func WrapperBenchmark(b *testing.B, batchSize int, fn GetRandomRequest) {
	fmt.Printf("Running Benchmark\n")

	// Number of total requests
	numRequests := b.N

	latenciesChannel := make(chan time.Duration, numRequests)

	b.ResetTimer()

	start := time.Now()
	last := time.Now()
	var ops atomic.Uint64
	runtime.GOMAXPROCS(20)
	threadId := 0

	/*
		Assuming GOMAXPROCS is not set, a 10-core CPU
		will run 10 Go-routines here.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {

		// Every go-routine will always query the same row
		threadId++

		// create a request
		verb, url, body := fn(b, batchSize)

		// One http connection per go-routine
		var httpClient *http.Client
		httpClient = testutils.SetupHttpClient(b)

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {

			requestStartTime := time.Now()

			testclient.SendHttpRequestWithClient(b, httpClient, verb, url, body, "", http.StatusOK)

			latenciesChannel <- time.Since(requestStartTime)
			count := ops.Add(1)
			if count%20000 == 0 {
				tempTotalPkLookups := 20000 * batchSize
				tempPkLookupsPerSecond := float64(tempTotalPkLookups) / time.Since(last).Seconds()
				b.Logf("Throughput:                 %f REST op/second", tempPkLookupsPerSecond)
				last = time.Now()
			}
		}
	})
	b.StopTimer()

	numTotalLookups := numRequests * batchSize
	opsPerSecond := float64(numTotalLookups) / time.Since(start).Seconds()

	latencies := make([]time.Duration, numRequests)
	for i := 0; i < numRequests; i++ {
		latencies[i] = <-latenciesChannel
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numRequests)*0.5)]
	p99 := latencies[int(float64(numRequests)*0.99)]

	b.Logf("Number of requests:         %d", numRequests)
	b.Logf("Batch size (per requests):  %d", batchSize)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f REST ops/second", opsPerSecond)
	b.Logf("50th percentile latency:    %v us", p50.Microseconds())
	b.Logf("99th percentile latency:    %v us", p99.Microseconds())
	b.Log("-------------------------------------------------")
}

// generate additional data in the fsdb002.sample_complex_type_512_1
func generateRandomComplexData(rows int, cols int) {

	schemaStr := `[
       "null",
       {
         "type": "array",
         "items": ["null", "long"]
       }
     ]`

	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		log.Fatalf("Failed to create codec: %v", err)
	}

	//random arrays
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < rows; i++ {
		testArray := map[string]interface{}{
			"array": generateArray(cols),
		}

		encodedData, err := codec.BinaryFromNative(nil, testArray)
		if err != nil {
			log.Fatalf("Failed to encode data: %v", err)
		}

		// Convert the encoded data to hex
		hexData := hex.EncodeToString(encodedData)

		// id := rand.Int63()
		id, err := uuid.NewRandom()
		if err != nil {
			fmt.Printf("Failed to generate UUID: %v\n", err)
			return
		}
		err = testutils.RunQueriesOnDataCluster(fmt.Sprintf("INSERT INTO fsdb002.sample_complex_type_512_1 VALUES ( \"%s\", 0x%s );\n", id.String(), hexData))
		if err != nil {
			fmt.Printf("Failed to insert data %v\n", err)
		}
	}
}

func generateArray(n int) []interface{} {
	rand.Seed(time.Now().UnixNano())
	array := make([]interface{}, n)

	for i := 0; i < n; i++ {
		if rand.Intn(2) == 0 {
			array[i] = nil // Randomly insert null
		} else {
			array[i] = map[string]interface{}{"long": rand.Int63()}
		}
	}

	return array
}
