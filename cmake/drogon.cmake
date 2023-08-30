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
  SET(USE_SUBMODULE OFF CACHE INTERNAL "Disable USE_SUBMODULE" FORCE)
  ADD_SUBDIRECTORY(${DROGON_SOURCE_DIR})
  SET(DROGON_INCLUDE_DIR ${DROGON_SOURCE_DIR}/lib/inc  CACHE INTERNAL "Drogon include dir" FORCE)
  SET(DROGON_ORM_INCLUDE_DIR ${DROGON_SOURCE_DIR}/orm_lib/inc  CACHE INTERNAL "(Drogon orm include dir" FORCE)
  SET(DROGON_NOSQL_INCLUDE_DIR ${DROGON_SOURCE_DIR}/nosql_lib/redis/inc CACHE INTERNAL "Drogon nosql include dir" FORCE)
  SET(DROGON_INCLUDE_DIR_BUILD ${CMAKE_CURRENT_BINARY_DIR}/extra/drogon/${DROGON_RELEASE}/exports CACHE INTERNAL "Drogon build include dir" FORCE)
  SET(DROGON_LIBRARY drogon CACHE INTERNAL "Bundled drogon library" FORCE)
ENDIF()

ENDMACRO()
