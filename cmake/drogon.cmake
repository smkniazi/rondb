# Copyright (c) 2023 Hopsworks and/or its affiliates.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

SET(DROGON_RELEASE drogon-1.8.4)
SET(DROGON_SOURCE_DIR ${CMAKE_SOURCE_DIR}/extra/drogon/${DROGON_RELEASE})

MACRO (CHECK_DROGON_WEB_FRAMEWORK)

  IF(WITH_RDRS)
    SET(USE_SUBMODULE OFF CACHE BOOL "Disable USE_SUBMODULE")
    ADD_SUBDIRECTORY(${DROGON_SOURCE_DIR})
    SET(DROGON_INCLUDE_DIR ${DROGON_SOURCE_DIR}/extra/drogon/lib/inc)
    SET(DROGON_LIBRARY dorgon CACHE INTERNAL "Bundled dorgon library")
    MESSAGE(STATUS "DROGON_INCLUDE_DIR ${DROGON_INCLUDE_DIR}")
    MESSAGE(STATUS "DROGON_LIBRARY ${DROGON_LIBRARY}")
  ENDIF()

ENDMACRO()
