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

#include "encoding.hpp"
#include "constants.hpp"
#include "rdrs_dal.hpp"
#include "buffer_manager.hpp"
#include <my_compiler.h>
#include "db_operations/pk/pkr_request.hpp"

#include <cstring>
#include <string>

/**
 * Build memory structure for Primary Key request. This is stored in
 * a serial buffer, the PKRRequest object is used as a way to easily
 * access this serial buffer making sure that the user of the serial
 * buffer is hidden from the internal logic of it.
 *
 * Here is the memory structure explained. All things are stored aligned
 * at 4 byte alignment, offsets, names and values.
 *
 * All names are stored null terminated as well as with a length word.
 *
 * The input data comes from the JSON request.
 *
 * ---------------------------------------
 * | Length of Database name             |
 * ---------------------------------------
 * | Database name                       |
 * ---------------------------------------
 * | Length of Table name                |
 * ---------------------------------------
 * | Table name                          |
 * ---------------------------------------
 * | Number of Primary key columns       |
 * ---------------------------------------
 * | Array of Tuple offset PK col 0..n-1 |
 * |                                     |
 * | Tuple offset points to name + value |
 * | offsets below.                      |
 * ---------------------------------------
 * ## Array of the following for each PK
 * ## column.
 * ---------------------------------------
 * | Offset of PK name column            |
 * | Offset of PK value                  |
 * ---------------------------------------
 * | Length of PK name                   |
 * |--------------------------------------
 * | PK column name                      |
 * |--------------------------------------
 * | PK value length                     |
 * ---------------------------------------
 * | PK Value                            |
 * ---------------------------------------
 * 
 * ---------------------------------------
 * | Number of read columns              |
 * ---------------------------------------
 * | Array of read column offsets 0..m-1 |
 * ---------------------------------------
 * ## Array of the following for each column
 * ## read. The above array points to the
 * ## offset of the start of this for
 * ## each read column.
 * ---------------------------------------
 * | Data type of read column            |
 * ---------------------------------------
 * | Length of read column name          |
 * ---------------------------------------
 * | Read column name                    |
 * ---------------------------------------
 */ 
RS_Status create_native_request(PKReadParams &pkReadParams,
                                Uint32 *reqBuff) {
  Uint32 *buf = reqBuff;
  Uint32 head = PK_REQ_HEADER_END;
  Uint32 dbOffset = head;
  EN_Status status = {};
  head = copy_str_to_buffer(pkReadParams.path.db, reqBuff, head, status);
  if (unlikely(head == 0)) {
    return CRS_Status(status.http_code, status.message).status;
  }
  Uint32 tableOffset = head;
  head = copy_str_to_buffer(pkReadParams.path.table, reqBuff, head, status);
  if (unlikely(head == 0)) {
    return CRS_Status(status.http_code, status.message).status;
  }
  // PK Filters
  Uint32 pkOffset = head;
  buf[head / ADDRESS_SIZE] = Uint32(pkReadParams.filters.size());
  head += ADDRESS_SIZE;
  Uint32 kvi = head / ADDRESS_SIZE;
  // index for storing offsets for each key/value pair
  // skip for N number of offsets one for each key/value pair
  head += Uint32(pkReadParams.filters.size()) * ADDRESS_SIZE;
  for (auto filter : pkReadParams.filters) {
    Uint32 tupleOffset = head;
    head += 8;
    Uint32 keyNameOffset = head;
    head = copy_str_to_buffer(filter.column, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
    Uint32 valueOffset = head;
    head = copy_ndb_str_to_buffer(filter.value, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
    buf[kvi] = tupleOffset;
    kvi++;
    buf[tupleOffset / ADDRESS_SIZE] = keyNameOffset;
    buf[tupleOffset / ADDRESS_SIZE + 1] = valueOffset;
  }
  // Read Columns
  Uint32 readColsOffset = 0;
  if (likely(!pkReadParams.readColumns.empty())) {
    readColsOffset = head;
    buf[head / ADDRESS_SIZE] = (Uint32)(pkReadParams.readColumns.size());
    head += ADDRESS_SIZE;
    Uint32 rci = head / ADDRESS_SIZE;
    head += Uint32(pkReadParams.readColumns.size()) * ADDRESS_SIZE;
    for (auto col : pkReadParams.readColumns) {
      buf[rci] = head;
      rci++;
      // return type not used for the moment
      Uint32 drt = DEFAULT_DRT;
      /*
      if (!col.returnType.empty()) {
        drt = data_return_type(col.returnType);
        if (drt == UINT32_MAX) {
          return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
            "Invalid return type").status;
        }
      }
      */
      buf[head / ADDRESS_SIZE] = drt;
      head += ADDRESS_SIZE;
      // col name
      head = copy_str_to_buffer(col.column, reqBuff, head, status);
      if (unlikely(head == 0)) {
        return CRS_Status(status.http_code, status.message).status;
      }
    }
  }
  // Operation ID
  Uint32 op_id_offset = 0;
  if (likely(!pkReadParams.operationId.empty())) {
    op_id_offset = head;
    head = copy_str_to_buffer(pkReadParams.operationId, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
  }
  // request buffer header
  buf[PK_REQ_OP_TYPE_IDX] = (Uint32)(RDRS_PK_REQ_ID);
  buf[PK_REQ_CAPACITY_IDX] = (Uint32)(globalConfigs.internal.respBufferSize);
  buf[PK_REQ_LENGTH_IDX] = (Uint32)(head);
  buf[PK_REQ_FLAGS_IDX] = (Uint32)(0);
  buf[PK_REQ_DB_IDX] = (Uint32)(dbOffset);
  buf[PK_REQ_TABLE_IDX] = (Uint32)(tableOffset);
  buf[PK_REQ_PK_COLS_IDX] = (Uint32)(pkOffset);
  buf[PK_REQ_READ_COLS_IDX] = (Uint32)(readColsOffset);
  buf[PK_REQ_OP_ID_IDX] = (Uint32)(op_id_offset);
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK), "OK").status;
}

RS_Status process_pkread_response(void *respBuff,
                                  RS_Buffer *reqBuff,
                                  PKReadResponseJSON &response) {
  PKRRequest req = PKRRequest(reqBuff);
  Uint32 *buf = (Uint32 *)(respBuff);
  Uint32 responseType = buf[PK_RESP_OP_TYPE_IDX];
  if (responseType != RDRS_PK_RESP_ID) {
    std::string msg = "internal server error. Wrong response type";
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      msg.c_str(), msg).status;
  }
  // some sanity checks
  Uint32 capacity   = buf[PK_RESP_CAPACITY_IDX];
  Uint32 dataLength = buf[PK_RESP_LENGTH_IDX];

  if (capacity < dataLength) {
    std::string message = "internal server error. response buffer"
      " may be corrupt. ";
    message += "Buffer capacity: " + std::to_string(capacity) +
               ", data length: " + std::to_string(dataLength);
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      message.c_str(), message).status;
  }
  Uint32 opIDX = buf[PK_RESP_OP_ID_IDX];
  if (opIDX != 0) {
    UintPtr opIDXPtr = (UintPtr)respBuff + (UintPtr)opIDX;
    std::string goOpID = std::string((char *)opIDXPtr);
    response.setOperationID(goOpID);
  }

  Int32 status = (Int32)(buf[PK_RESP_OP_STATUS_IDX]);
  response.setStatusCode(static_cast<drogon::HttpStatusCode>(status));
  if (status == drogon::HttpStatusCode::k200OK) {
    Uint32 colIDX = buf[PK_RESP_COLS_IDX];
    UintPtr colIDXPtr = (UintPtr)respBuff + (UintPtr)colIDX;
    Uint32 colCount = *(Uint32 *)colIDXPtr;
    for (Uint32 i = 0; i < colCount; i++) {
      Uint32 *colHeaderStart = reinterpret_cast<Uint32 *>(
        reinterpret_cast<UintPtr>(respBuff) + colIDX + ADDRESS_SIZE +
          i * 4 * ADDRESS_SIZE);

      Uint32 colHeader[4];
      for (Uint32 j = 0; j < 4; j++) {
        colHeader[j] = colHeaderStart[j];
      }
      //Uint32 nameAdd = colHeader[0];
      std::string name = req.ReadColumnName(i);
      Uint32 valueAdd = colHeader[1];
      Uint32 isNull = colHeader[2];
      Uint32 dataType = colHeader[3];
      if (isNull == 0) {
        std::string value =
          std::string((char *)
            (reinterpret_cast<UintPtr>(respBuff) + valueAdd));
        std::string quotedValue = quote_if_string(dataType, value);
        response.setColumnData(
          name, std::vector<char>(quotedValue.begin(), quotedValue.end()));
      } else {
        response.setColumnData(name, std::vector<char>());
      }
    }
  }
  std::string message = "";
  Uint32 messageIDX = buf[PK_RESP_OP_MESSAGE_IDX];
  if (messageIDX != 0) {
    UintPtr messageIDXPtr = (UintPtr)respBuff + (UintPtr)messageIDX;
    message = std::string((char *)messageIDXPtr);
  }
  return CRS_Status(static_cast<HTTP_CODE>(status), message.c_str()).status;
}
