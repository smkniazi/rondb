/*
 * Copyright (C) 2023 Hopsworks AB
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

#include "logger.hpp"

#include <string>
#include <cstring>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

namespace rdrs_logger {

void log(const int level, const char *msg) {
    if (level <= ErrorLevel) {
      g_eventLogger->error("%s", msg);
    } else if (level <= WarnLevel) {
      g_eventLogger->warning("%s", msg);
    } else if (level <= InfoLevel) {
      g_eventLogger->info("%s", msg);
    } else {
      g_eventLogger->debug("%s", msg);
    }
}

void error(const char *msg) {
  log(ErrorLevel, msg);
}

void error(const std::string msg) {
  log(ErrorLevel, msg.c_str());
}

void warn(const char *msg) {
  log(WarnLevel, msg);
}

void warn(const std::string msg) {
  log(WarnLevel, msg.c_str());
}

void info(const char *msg) {
  log(InfoLevel, msg);
}

void info(const std::string msg) {
  log(InfoLevel, msg.c_str());
}

void debug(const char *msg) {
  log(DebugLevel, msg);
}

void debug(const std::string msg) {
  log(DebugLevel, msg.c_str());
}

}  // namespace rdrs_logger
