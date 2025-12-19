/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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

#include "scan_read_ctrl.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "pk_data_structs.hpp"
#include "api_key.hpp"
#include "src/constants.hpp"
#include "metrics.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>
#include <util/require.h>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_SCAN_CTRL 1
#endif

#ifdef DEBUG_SCAN_CTRL
#define DEB_SCAN_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_SCAN_CTRL(...) do { } while (0)
#endif


#include <rapidjson/document.h>      // rapidjson::Document
#include <rapidjson/stringbuffer.h>

typedef rapidjson::UTF8<char> RJ_Encoding;
typedef rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> RJ_Allocator;
typedef rapidjson::GenericDocument<RJ_Encoding, RJ_Allocator,
                                   rapidjson::CrtAllocator> RJ_Document;
typedef rapidjson::GenericStringBuffer<RJ_Encoding,
                                       rapidjson::CrtAllocator> RJ_StringBuffer;

void ScanReadCtrl::ScanRead(
       const drogon::HttpRequestPtr& req,
       std::function<void(const drogon::HttpResponsePtr &)>&& callback,
       const std::string_view& db,
       const std::string_view& table) {

  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  // TODO (Zhao)
  // BatchPkReadEndPointMetricsUpdater metricsUpdater(resp);
  bool use_compressed = globalConfigs.rest.useCompression;

  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
#ifdef DEBUG_SCAN_CTRL
  printf("\n\n JSON REQUEST: \n %s \n", json_str);
#endif
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.maxReqSize)) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer().get(), json_str, length);

  ScanReadParams reqStruct(db, table);

  RS_Status status = jsonParser.scan_parse(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.maxReqSize +
                                   simdjson::SIMDJSON_PADDING),
                                   reqStruct);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validation
  status = validate_db(reqStruct.path.db);
  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  const std::string_view table_view = reqStruct.path.table;
  status = validate_table(table_view);
  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (!reqStruct.readColumns.empty()) {
    status = ValidateScanColumns(reqStruct.readColumns);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  if (reqStruct.filterRoot) {
    status = ValidateScanFilter(reqStruct.filterRoot);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  if (reqStruct.index != std::nullopt) {
    status = ValidateScanIndex(reqStruct.index.value());
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  // Authenticate
  std::vector<std::string_view> db_vector;
  db_vector.push_back(reqStruct.path.db);
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status = authenticate(api_key, db_vector);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
  }

  RJ_Document doc;
  RJ_StringBuffer buf;
  // TODO (Zhao)
  buf.Reserve(256 * 1024);
  status = scan_read(reqStruct, currentThreadIndex, (void*)&buf);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  resp->setBody(std::string(buf.GetString(), buf.GetSize()));
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
  return;
}
