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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_

#include "rdrs_dal.h"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>
#include <ndb_types.h>

namespace rdrs_metrics {

void initMetrics();
void writeMetrics(drogon::HttpResponsePtr resp);
void incrementEndpointAccessCount(const char *endPointLabel, const char *methodType, Uint32 status);
void observeEndpointLatency(const char *endPointLabel, const char *methodType, Uint64 latency);
void setRonDBStats();

// Class for Updating the endpoints stats
class EndPointMetricsUpdater {
 private:
  EndPointMetricsUpdater()                                          = delete;
  EndPointMetricsUpdater(const EndPointMetricsUpdater &)            = delete;
  EndPointMetricsUpdater &operator=(const EndPointMetricsUpdater &) = delete;
  EndPointMetricsUpdater(EndPointMetricsUpdater &&)                 = delete;
  EndPointMetricsUpdater &operator=(EndPointMetricsUpdater &&)      = delete;

  const char *endPointLabel;
  const char *endPointVerb;
  drogon::HttpResponsePtr response;
  std::chrono::high_resolution_clock::time_point start;

 public:
  EndPointMetricsUpdater(const char *endPointLabel, const char *endPointVerb,
                         drogon::HttpResponsePtr response)
      : endPointLabel(endPointLabel), endPointVerb(endPointVerb), response(std::move(response)),
        start(std::chrono::high_resolution_clock::now()) {
  }

  ~EndPointMetricsUpdater() {
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    Uint64 elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    observeEndpointLatency(endPointLabel, endPointVerb, elapsed_ns);
    incrementEndpointAccessCount(endPointLabel, endPointVerb, response->getStatusCode());
  }
};

}  // namespace rdrs_metrics

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_
