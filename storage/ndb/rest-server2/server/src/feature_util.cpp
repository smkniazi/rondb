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
#include "feature_util.hpp"
#include "prometheus_ctrl.hpp"
#include <drogon/HttpTypes.h>
#include <memory>
#include <optional>
#include <simdjson.h>
#include <vector>

RS_Status
base64_decode(const std::string &encoded_string, std::string &decoded_string) {
  const char *src = encoded_string.c_str();
  size_t src_len  = encoded_string.size();
  std::vector<char> decoded_data(src_len);  // Allocate enough space

  const char *end_ptr = nullptr;
  int flags           = 0;  // No special flags
  Int64 decoded_len =
      base64_decode(src, src_len, decoded_data.data(), &end_ptr, flags);

  if (decoded_len < 0) {
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
                      "Failed to decode base64 string.")
        .status;
  }

  decoded_string.assign(decoded_data.begin(),
                        decoded_data.begin() + decoded_len);
  return CRS_Status::SUCCESS.status;
}

std::tuple<std::vector<char>, std::shared_ptr<RestErrorCode>>
DeserialiseComplexFeature(const std::vector<char> &value,
                          const metadata::AvroDecoder &decoder) {
  std::string valueString(value.begin(), value.end());
  simdjson::dom::parser parser;
  simdjson::dom::element element;

  auto error = parser.parse(valueString).get(element);
  if (error != 0U) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
            "Failed to unmarshal JSON value.",
            static_cast<int>(drogon::k500InternalServerError)));
  }

  valueString = element.get_string().value();
  std::string jsonDecode;
  RS_Status status = base64_decode(valueString, jsonDecode);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
            status.message, static_cast<int>(drogon::k500InternalServerError)));
  }

  std::vector<Uint8> binaryData(jsonDecode.begin(), jsonDecode.end());
  auto [native_status, native] = decoder.decode(binaryData);
  if (native_status.http_code != HTTP_CODE::SUCCESS) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
            native_status.message, static_cast<int>(drogon::k400BadRequest)));
  }

  auto [json_status, json] = ConvertAvroToJson(native.value());
  if (json_status.http_code != HTTP_CODE::SUCCESS) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
            "Failed to convert Avro to JSON.",
            static_cast<int>(drogon::k500InternalServerError)));
  }
  return std::make_tuple(json.value(), nullptr);
}

template <typename T>
void
AppendToVector(std::vector<char> &vec, const T &value) {
  const char *data = reinterpret_cast<const char *>(&value);
  vec.insert(vec.end(), data, data + sizeof(T));
}

void
AppendStringToVector(std::vector<char> &vec, const std::string &str) {
  vec.insert(vec.end(), str.begin(), str.end());
}

void
AppendBytesToVector(std::vector<char> &vec, const std::vector<Uint8> &bytes) {
  vec.insert(vec.end(), bytes.begin(), bytes.end());
}

// Recursive function to process Avro data
RS_Status
processDatum(const avro::GenericDatum &datum, std::ostringstream &oss) {
  switch (datum.type()) {
  case avro::AVRO_NULL:
    oss << "null";
    break;
  case avro::AVRO_BOOL:
    oss << (datum.value<bool>() ? "true" : "false");
    break;
  case avro::AVRO_INT:
    oss << datum.value<int32_t>();
    break;
  case avro::AVRO_LONG:
    oss << datum.value<int64_t>();
    break;
  case avro::AVRO_FLOAT:
    oss << datum.value<float>();
    break;
  case avro::AVRO_DOUBLE:
    oss << datum.value<double>();
    break;
  case avro::AVRO_STRING:
    oss << "\"" << datum.value<std::string>() << "\"";
    break;
  case avro::AVRO_RECORD: {
    const auto &record = datum.value<avro::GenericRecord>();
    oss << "{";
    for (size_t i = 0; i < record.fieldCount(); ++i) {
      if (i > 0) {
        oss << ",";
      }
      oss << "\"" << record.schema()->nameAt(i) << "\":";
      processDatum(record.fieldAt(i), oss);
    }
    oss << "}";
    break;
  }
  case avro::AVRO_ARRAY: {
    const auto &array = datum.value<avro::GenericArray>();
    oss << "[";
    bool first = true;
    for (const auto &element : array.value()) {
      if (!first) {
        oss << ",";
      }
      first = false;
      processDatum(element, oss);
    }
    oss << "]";
    break;
  }
  case avro::AVRO_MAP: {
    const auto &map = datum.value<avro::GenericMap>();
    oss << "{";
    bool first = true;
    for (const auto &entry : map.value()) {
      if (!first) {
        oss << ",";
      }
      first = false;
      oss << "\"" << entry.first << "\":";
      processDatum(entry.second, oss);
    }
    oss << "}";
    break;
  }
  case avro::AVRO_ENUM:
    oss << "\"" << datum.value<avro::GenericEnum>().symbol() << "\"";
    break;
  case avro::AVRO_UNION: {
    const auto &unionData = datum.value<avro::GenericUnion>();
    processDatum(unionData.datum(),
                 oss);  // Unwrap the union and process its actual value
    break;
  }
  case avro::AVRO_FIXED:
    oss << "\""
        << std::string(datum.value<avro::GenericFixed>().value().begin(),
                       datum.value<avro::GenericFixed>().value().end())
        << "\"";
    break;
  default:
    CRS_Status(HTTP_CODE::SERVER_ERROR,
               "Failed to convert avro::GenericDatum to JSON");
  }
  return CRS_Status::SUCCESS.status;
}

// Convert Avro data to JSON
std::pair<RS_Status, std::optional<std::vector<char>>>
ConvertAvroToJson(const avro::GenericDatum &datum) {
  try {

    // Convert Avro data to JSON
    std::ostringstream oss;
    RS_Status status = processDatum(datum, oss);
    if (status.http_code != HTTP_CODE::SUCCESS) {
      return {status, std::nullopt};

    } else {
      std::string finalJsonStr = oss.str();
      return {CRS_Status::SUCCESS.status,
              std::vector<char>{finalJsonStr.begin(), finalJsonStr.end()}};
    }

  } catch (const std::exception &e) {
    return {
      CRS_Status(HTTP_CODE::SERVER_ERROR,
                 "Exception occurred: " + std::string(e.what()))
          .status,
          std::nullopt};
    }
  }

