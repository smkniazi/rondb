/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_

#include "rdrs_dal.h"
#include <ndb_types.h>

#include <drogon/HttpTypes.h>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <unordered_map>

std::string to_string(DataReturnType);
Uint32 decode_utf8_to_unicode(const std::string_view &, size_t &);
RS_Status validate_db_identifier(const std::string_view &);
RS_Status validate_operation_id(const std::string &);
RS_Status validate_db(const std::string_view);
RS_Status validate_table(const std::string_view);
RS_Status validate_column(const std::string_view);

class PKReadFilter {
 public:
  //std::string_view column;
  std::string_view column;
  std::vector<char> value;
};

class PKReadReadColumn {
 public:
  std::string_view column;
};

class PKReadPath {
 public:
  PKReadPath();
  PKReadPath(const std::string_view &, const std::string_view &);
  // json:"db" uri:"db"  binding:"required,min=1,max=64"
  std::string_view db;
  // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
  std::string table;
};

class PKReadParams {
 public:
  PKReadParams();
  explicit PKReadParams(const std::string_view &);
  explicit PKReadParams(PKReadPath &);
  PKReadParams(const std::string_view &, const std::string_view &);
  PKReadPath path;
  std::vector<PKReadFilter> filters;
  std::vector<PKReadReadColumn> readColumns;
  std::string operationId;
  std::string to_string();
  RS_Status validate();
  RS_Status validate_columns();
};

struct Column {
  std::string_view name;
  std::vector<char> value;  // Byte array for the value
};

struct ResultView {
  const char *name_ptr;
  Uint32 name_len;
  const char *value_ptr;
  Uint32 value_len;
  bool quoted_flag;
};

class PKReadResponse {
 public:
  PKReadResponse()= default;
  virtual void init(Uint32, ResultView*) = 0;
  virtual void setOperationID(const char*, Uint32) = 0;
  virtual void setOperationID(std::string_view) = 0;
  virtual void setColumnData(Uint32 index,
                             const char *name,
                             Uint32 name_len,
                             const char *value,
                             Uint32 value_len,
                             bool quoted_flag) = 0;
  virtual std::string to_string() const = 0;
  virtual ~PKReadResponse() = default;
};

class PKReadResponseJSON : public PKReadResponse {
 private:
  // json:"code"    form:"code"    binding:"required"
  drogon::HttpStatusCode code;
  // json:"operationId" form:"operation-id" binding:"omitempty"
  const char *opIdPtr;
  Uint32 opIdLen;
  Uint32 num_values;
  // json:"data" form:"data" binding:"omitempty"
  ResultView *result_view;

 public:
  PKReadResponseJSON() : PKReadResponse() {
  }

  PKReadResponseJSON(const PKReadResponseJSON &other) : PKReadResponse() {
    code = other.code;
    num_values = other.num_values;
    opIdPtr = other.opIdPtr;
    opIdLen = other.opIdLen;
    result_view = other.result_view;
  }
  PKReadResponseJSON &operator=(const PKReadResponseJSON &other) {
    code = other.code;
    num_values = other.num_values;
    opIdPtr = other.opIdPtr;
    opIdLen = other.opIdLen;
    result_view = other.result_view;
    return *this;
  }
  void init(Uint32 numColumns,
            ResultView *in_result_view) override {
    code = drogon::HttpStatusCode::kUnknown;
    opIdPtr = nullptr;
    opIdLen = 0;
    num_values = numColumns;
    result_view = in_result_view;
  }
  void setStatusCode(drogon::HttpStatusCode c) {
    code = c;
  }
  void setOperationID(const char *opId, Uint32 len) override {
    opIdPtr = opId;
    opIdLen = len;
  }
  void setOperationID(std::string_view str_view) override {
    opIdPtr = str_view.data();
    opIdLen = str_view.size();
  }
  void setColumnData(Uint32 index,
                     const char *name,
                     Uint32 name_len,
                     const char *value,
                     Uint32 value_len,
                     bool quoted) override {
    result_view[index].name_ptr = name;
    result_view[index].name_len = name_len;
    result_view[index].value_ptr = value;
    result_view[index].value_len = value_len;
    result_view[index].quoted_flag = quoted;
  }
  drogon::HttpStatusCode getStatusCode() const {
    return code;
  }
  std::string_view getOperationID() const {
    std::string_view opId(opIdPtr, opIdLen);
    return opId;
  }
  std::string_view getName(Uint32 index) const {
    std::string_view name(result_view[index].name_ptr,
                          result_view[index].name_len);
    return name;
  }
  std::string getOperationIdString() const {
    std::string opId(opIdPtr, opIdLen);
    return opId;
  }
  Uint32 getNumValues() const {
    return num_values;
  }
  std::string getNameString(Uint32 index) const {
    std::string name(result_view[index].name_ptr,
                     result_view[index].name_len);
    return name;
  }
  std::string getValueString(Uint32 index) const {
    std::string value(result_view[index].value_ptr,
                      result_view[index].value_len);
    return value;
  }
  std::vector<char> getValueArray(Uint32 index) {
    const char *ptr = result_view[index].value_ptr;
    Uint32 len = result_view[index].value_len;
    std::vector<char> vec(ptr, ptr + len);
    return vec;
  }
  std::string_view getValue(Uint32 index) const {
    std::string_view value(result_view[index].value_ptr,
                           result_view[index].value_len);
    return value;
  }
  bool getQuoteFlag(Uint32 index) {
    return result_view[index].quoted_flag;
  }

  std::string to_string() const override;
  std::string to_string(int, bool) const;
  static std::string batch_to_string(const std::vector<PKReadResponseJSON> &);
};

class PKReadResponseWithCodeJSON {
 private:
  // json:"message"    form:"message"    binding:"required"
  std::string message;
  // json:"body"    form:"body"    binding:"required"
  PKReadResponseJSON body;

 public:
  PKReadResponseWithCodeJSON() = default;

  PKReadResponseWithCodeJSON(const PKReadResponseWithCodeJSON &other) {
    message = other.message;
    body = other.body;
  }

  PKReadResponseWithCodeJSON &operator=(
    const PKReadResponseWithCodeJSON &other) {
    message = other.message;
    body = other.body;
    return *this;
  }

  void setMessage(std::string &msg) {
    message = msg;
  }

  void setMessage(const char *msg) {
    message = msg;
  }

  void setBody(const PKReadResponseJSON &b) {
    body = b;
  }

  void setOperationId(const char *opId, Uint32 opIdLen) {
    body.setOperationID(opId, opIdLen);
  }

  void setOperationId(std::string_view str_view) {
    body.setOperationID(str_view);
  }

  std::string getMessage() const {
    return message;
  }

  PKReadResponseJSON getBody() const {
    return body;
  }

  std::string to_string() const;
};

class BatchResponseJSON {
 private:
  // json:"result" binding:"required"
  std::vector<PKReadResponseWithCodeJSON> result;

 public:
  BatchResponseJSON() = default;

  BatchResponseJSON(const BatchResponseJSON &other) : result(other.result) {
  }

  BatchResponseJSON &operator=(const BatchResponseJSON &other) {
    if (this != &other) {
      result = other.result;
    }
    return *this;
  }

  void setResult(const std::vector<PKReadResponseWithCodeJSON> &res) {
    result = res;
  }

  std::vector<PKReadResponseWithCodeJSON> getResult() const {
    return result;
  }

  void Init(int numSubResponses) {
    result.resize(numSubResponses);
  }

  static PKReadResponseWithCodeJSON CreateNewSubResponse() {
    PKReadResponseWithCodeJSON subResponse;
    return subResponse;
  }

  void AddSubResponse(unsigned long index,
                      const PKReadResponseWithCodeJSON &subResp) {
    if (index < result.size()) {
      result[index] = subResp;
    }
  }

  std::string to_string() const {
    std::string res = "[";
    for (size_t i = 0; i < result.size(); i++) {
      res += result[i].to_string();
      if (i < result.size() - 1) {
        res += ",";
      }
    }
    res += "]";
    return res;
  }
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
