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

#include "pkr_request.hpp"
#include "src/logger.hpp"
#include "src/rdrs_const.h"
#include "src/status.hpp"
#include "my_compiler.h"

PKRRequest::PKRRequest(const RS_Buffer *request) {
  this->req = request;
  this->isInvalidOp = false;
}

Uint32 PKRRequest::OperationType() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_OP_TYPE_IDX];
}

Uint32 PKRRequest::Length() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_LENGTH_IDX];
}

Uint32 PKRRequest::Capacity() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_CAPACITY_IDX];
}

const char *PKRRequest::DB() {
  Uint32 dbOffset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_DB_IDX];
  return req->buffer + dbOffset + 4;
}

const char *PKRRequest::Table() {
  Uint32 tableOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_TABLE_IDX];
  return req->buffer + tableOffset + 4;
}

Uint32 PKRRequest::PKColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  Uint32 count  =
    (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
  return count;
}

Uint32 PKRRequest::PKTupleOffset(const int n) {
  // [count][kv offset1]...[kv offset n] [k offset][v offset] [ bytes ... ]
  // [k offset][v offset]...
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  //
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  // +1 for count
  Uint32 kvOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  return kvOffset;
}

const char *PKRRequest::PKName(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[kvOffset / 4];
  return req->buffer + kOffset + ADDRESS_SIZE;
}

Uint32 PKRRequest::PKNameLen(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[kvOffset / 4];
  const char *ptr = req->buffer + kOffset;
  const Uint32 *len_ptr = reinterpret_cast<const Uint32*>(ptr);
  return *len_ptr;
}

const char *PKRRequest::PKValueCStr(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset  =
    (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  // skip first 4 bytes that contain size of string
  return req->buffer + vOffset + 4;
}

/*
  PKValueLen refers to data without prepended length bytes.
  The length bytes are only native to RonDB.
*/
Uint32 PKRRequest::PKValueLen(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  unsigned char *data_start = (unsigned char *)req->buffer + vOffset;
  Uint32 *len_ptr = reinterpret_cast<Uint32*>(data_start);
  return *len_ptr;
}

Uint32 PKRRequest::ReadColumnsCount() {
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  if (unlikely(offset == 0)) {
    return 0;
  } else {
    Uint32 count =
      (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
    return count;
  }
}

const char *PKRRequest::ReadColumnName(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  //   [ return type ] [ bytes ... ]
  //                                                         
  //          ......................................| 
  //                         .............................................|

  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 r_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  return req->buffer + r_offset + (ADDRESS_SIZE * 2);
}

Uint32 PKRRequest::ReadColumnNameLen(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  //   [ return type ] [ bytes ... ]
  //                                                         
  //          ......................................| 
  //                         .............................................|

  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 r_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  const char *ptr = req->buffer + r_offset + ADDRESS_SIZE;
  const Uint32 *len_ptr = reinterpret_cast<const Uint32*>(ptr);
  return *len_ptr;
}

DataReturnType PKRRequest::ReadColumnReturnType(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  // [ return type ] [ bytes ... ]
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 c_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  Uint32 type =
    (reinterpret_cast<Uint32 *>(req->buffer))[c_offset / ADDRESS_SIZE];
  return static_cast<DataReturnType>(type);
}

const char *PKRRequest::OperationId() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_OP_ID_IDX];
  if (likely(offset != 0)) {
    return req->buffer + offset + 4;
  } else {
    return nullptr;
  }
}

void PKRRequest::MarkInvalidOp(RS_Status error) {
  this->error = error;
  this->isInvalidOp = true;
}

RS_Status PKRRequest::GetError() {
  return this->error;
}

bool PKRRequest::IsInvalidOp() {
  return this->isInvalidOp;
}
