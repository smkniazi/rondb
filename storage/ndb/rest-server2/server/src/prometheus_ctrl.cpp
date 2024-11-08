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

#include "prometheus_ctrl.hpp"
#include "rdrs_dal.h"
//#include "logger.hpp"

#include <drogon/HttpTypes.h>
#include <prometheus/text_serializer.h>
#include <prometheus/counter.h>
#include <prometheus/summary.h>
#include <prometheus/family.h>
#include <prometheus/info.h>
#include <prometheus/registry.h>
#include <string>
#include <my_compiler.h>  // for likely and unlikely

using namespace prometheus;

//using namespace RDRSLogger;

namespace rdrs_metrics {

// Redefined. in go version pk-read enpoint metrics are reported as "PkRead" :(
// Changing labels require changing the dash boards in  Hopsworks Monitoring
static std::string PING_EP_LABEL     = std::string("ping");
static std::string HEALTH_EP_LABEL   = std::string("health");
static std::string PKREAD_EP_LABEL   = std::string("PkRead");
static std::string BATCH_EP_LABEL    = std::string("batch");
static std::string POST_METHOD_LABEL = std::string("POST");
static std::string GET_METHOD_LABEL  = std::string("GET");

static std::shared_ptr<Registry> registry                      = std::make_shared<Registry>();
static prometheus::Family<prometheus::Counter> *requestCounter = nullptr;
static prometheus::Family<prometheus::Summary> *summary_family = nullptr;
static prometheus::Gauge *ronDBConnectionStateGauge            = nullptr;
static prometheus::Gauge *ndbObjectsTotalCountGauge            = nullptr;
static prometheus::Summary::Quantiles objectives               = {
    {0.5, 0.05},   // 50th percentile with 5% error margin
    {0.9, 0.01},   // 90th percentile with 1% error margin
    {0.95, 0.01},  // 95th percentile with 1% error margin
    {0.99, 0.001}  // 99th percentile with 0.1% error margin
};

// struct EndPointMetrics {
// Summary* ResponseTimeSummary;
// Counter* ResponseStatusCount;
// };

void initMetrics() {
  requestCounter = &BuildCounter()
                        .Name("rdrs_endpoints_response_status_count")
                        .Help("Number of response status returned by REST API")
                        .Register(*registry);

  summary_family =
      &prometheus::BuildSummary()
           .Name("rdrs_endpoints_response_time_summary")
           .Help("Summary for response time handled by REST API handler. Time is in nanoseconds")
           .Register(*registry);

  ronDBConnectionStateGauge = &prometheus::BuildGauge()
                                   .Name("rdrs_rondb_connection_state")
                                   .Help("Connection state (0: connected, > 0  not connected)")
                                   .Register(*registry)
                                   .Add({});

  ndbObjectsTotalCountGauge = &prometheus::BuildGauge()
                                   .Name("rdrs_rondb_total_ndb_objects")
                                   .Help("Total NDB objects")
                                   .Register(*registry)
                                   .Add({});
}

void setRonDBStats() {
  RonDB_Stats stats;
  RS_Status status = get_rondb_stats(&stats);

  if (likely(status.http_code != SUCCESS)) {
    ndbObjectsTotalCountGauge->Set(stats.ndb_objects_count);
    ronDBConnectionStateGauge->Set(stats.connection_state);
  } else {
    // RDRSLogger::LOG_ERROR("Failed to read metrics for RonDB");
  }
}

void incrementEndpointAccessCount(std::string endPointLabel, std::string methodType, int status) {
  std::string statusStr = std::to_string(status);
  // create a new counter or return an already existing one
  prometheus::Counter *pkCounter = &requestCounter->Add({{"api_type", "REST"},
                                                         {"end_point", endPointLabel},
                                                         {"method", methodType},
                                                         {"status", statusStr}});
  pkCounter->Increment();
}

void observeEndpointLatency(std::string endPointLabel, std::string methodType, int latency) {
  // create a new counter or return an already existing one
  prometheus::Summary &response_time_summary = summary_family->Add(
      {{"api_type", "REST"}, {"end_point", endPointLabel}, {"method", methodType}}, objectives);
  response_time_summary.Observe(latency);
}

void PrometheusCtrl::metrics(const drogon::HttpRequestPtr &req,
                             std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  incrementEndpointAccessCount(PKREAD_EP_LABEL, POST_METHOD_LABEL, 200);
  incrementEndpointAccessCount(PKREAD_EP_LABEL, POST_METHOD_LABEL, 201);
  for (int i = 0; i < 100; i++)
    observeEndpointLatency(PKREAD_EP_LABEL, POST_METHOD_LABEL, rand() % 100);
  auto resp = drogon::HttpResponse::newHttpResponse();
  prometheus::TextSerializer serializer;
  std::ostringstream os;
  serializer.Serialize(os, registry->Collect());

  // Create an HTTP response with the serialized metrics
  resp->setBody(os.str());
  resp->setContentTypeString("text/plain; version=0.0.4");
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
  printf("metrics called\n");
}

}  // namespace rdrs_metrics
