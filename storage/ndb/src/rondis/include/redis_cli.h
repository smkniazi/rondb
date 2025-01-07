// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef PINK_INCLUDE_REDIS_CLI_H_
#define PINK_INCLUDE_REDIS_CLI_H_

#include <vector>
#include <string>

namespace pink {


typedef std::vector<std::string> RedisCmdArgsType;
// We can serialize redis command by 2 ways:
// 1. by variable argmuments;
//    eg.  RedisCli::Serialize(cmd, "set %s %d", "key", 5);
//        cmd will be set as the result string;
// 2. by a string vector;
//    eg.  RedisCli::Serialize(argv, cmd);
//        also cmd will be set as the result string.
extern int SerializeRedisCommand(std::string *cmd, const char *format, ...);
extern int SerializeRedisCommand(RedisCmdArgsType argv, std::string *cmd);

}   // namespace pink

#endif  // PINK_INCLUDE_REDIS_CLI_H_
