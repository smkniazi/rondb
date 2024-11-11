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
#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOGGER_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOGGER_HPP_

#include "rdrs_dal.h"

#include <string>

namespace rdrs_logger {

// Undefine the conflicting macros in trantor
#ifdef LOG_DEBUG
#undef LOG_DEBUG
#endif
#ifdef LOG_INFO
#undef LOG_INFO
#endif
#ifdef LOG_WARN
#undef LOG_WARN
#endif

#define ErrorLevel 2
#define WarnLevel  3
#define InfoLevel  4
#define DebugLevel 5
#define TraceLevel 6

void log(const int level, const char *msg);

void error(const char *msg);

void error(const std::string msg);

void warn(const char *msg);

void warn(const std::string msg);

void info(const char *msg);

void info(const std::string msg);

void debug(const char *msg);

void debug(const std::string msg);

}  // namespace rdrs_logger

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOGGER_HPP_
