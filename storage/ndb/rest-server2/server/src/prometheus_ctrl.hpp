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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROMETHEUS_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROMETHEUS_CTRL_HPP_

#include "rdrs_dal.h"
#include "constants.hpp"

#include "logger.hpp"
#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>
#include <ndb_types.h>

namespace rdrs_metrics {
class PrometheusCtrl : public drogon::HttpController<PrometheusCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(PrometheusCtrl::metrics, PROMETHEUS_METRICS_PATH, drogon::Get);
  METHOD_LIST_END

  static void metrics(const drogon::HttpRequestPtr &req,
                      std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

void initMetrics();
void incrementEndpointAccessCount(std::string endPointLabel, std::string methodType, int status);
void observeEndpointLatency(std::string endPointLabel, std::string methodType, int latency);
void setRonDBStats();
}  // namespace rdrs_metrics

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROMETHEUS_CTRL_HPP_
