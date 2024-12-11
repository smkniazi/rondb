if(NOT WITH_RDRS)
  MESSAGE(STATUS "Skip building REST Server") 
  RETURN()
endif()

file(GLOB_RECURSE SRC
        api_key.cpp
        base_ctrl.cpp
        batch_feature_store_ctrl.cpp
        batch_pk_read_ctrl.cpp
        buffer_manager.cpp
        config_structs.cpp
        connection.cpp
        encoding.cpp
        encoding_helper.cpp
        feature_store_ctrl.cpp
        feature_store_data_structs.cpp
        feature_util.cpp
        feature_store/feature_store.cpp
        fs_cache.cpp
        health_ctrl.cpp
        json_parser.cpp
        json_printer.cpp
        logger.cpp
        main.cc
        metadata.cpp
        metrics.cpp
        mystring.cpp
        ndb_api_helper.cpp
        operations_feature_store.cpp
        ping_ctrl.cpp
        pk_data_structs.cpp
        pk_read_ctrl.cpp
        pk_read_ctrl.cpp
        prometheus_ctrl.cpp
        rdrs_dal.cpp
        rdrs_hopsworks_dal.cpp
        rdrs_rondb_connection.cpp
        rdrs_rondb_connection_pool.cpp
        retry_handler.cpp
        ronsql_ctrl.cpp
        tls_util.cpp
        db_operations/pk/common.cpp
        db_operations/pk/pkr_operation.cpp
        db_operations/pk/pkr_request.cpp
        db_operations/pk/pkr_response.cpp
        db_operations/ronsql/ronsql_operation.cpp
)
NDB_ADD_EXECUTABLE(${PROJECT_NAME} ${SRC} STATIC_NDBCLIENT)
add_dependencies(${PROJECT_NAME} DROGON_IS_BUILD SIMDJSON_IS_BUILD)

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR_64})
find_package(Drogon CONFIG REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR_64})
find_package(simdjson REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR_64})

find_path(AVROCPP_INCLUDE_DIR avro/ValidSchema.hh
  PATHS /usr/local/include /usr/include /opt/homebrew/include
)

find_library(AVROCPP_LIBRARIES NAMES avrocpp
  PATHS /usr/local/lib /usr/lib /opt/homebrew/lib
)

if(AVROCPP_INCLUDE_DIR AND AVROCPP_LIBRARIES)
    message(STATUS "Found avrocpp: ${AVROCPP_LIBRARIES}")
else()
  message(STATUS "avrocpp NOT FOUND")
  message("Please refer to the latest Avro C++ Installing instructions: https://avro.apache.org/docs/1.12.0/api/cpp/html/")
  if(APPLE)
      message("or  brew install avro-cpp")
  endif()

  message(FATAL_ERROR "Failed to configure rdrs2. avrocpp NOT FOUND")
endif()

find_library(OSSP_UUID_LIBRARY NAMES ossp-uuid
  PATHS /lib64 /usr/lib64
)

find_library(UUID_LIBRARY NAMES uuid
  PATHS /lib64 /usr/lib64
)

find_library(BOOST_FS NAMES boost_filesystem
  PATHS /usr/local/lib /usr/local/lib64
)

find_library(BOOST_IO NAMES boost_iostream
  PATHS /usr/local/lib /usr/local/lib64
)

find_library(BOOST_PO NAMES boost_program_options
  PATHS /usr/local/lib /usr/local/lib64
)

find_library(BOOST_RE NAMES boost_regex
  PATHS /usr/local/lib /usr/local/lib64
)

find_library(BOOST_SY NAMES boost_system
  PATHS /usr/local/lib /usr/local/lib64
)

find_library(BOOST_AT NAMES boost_atomic
  PATHS /usr/local/lib /usr/local/lib64
)

find_package(Jsoncpp REQUIRED)
find_package(Threads REQUIRED)

#prometheus-cpp
find_package(prometheus-cpp CONFIG QUIET)
if (NOT prometheus-cpp_FOUND)
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    message(FATAL_ERROR "Error: prometheus-cpp was not found. Please install prometheus-cpp. \n\t\t brew install prometheus-cpp \n\t\t or install from src https://github.com/jupp0r/prometheus-cpp")
  else()
    message(FATAL_ERROR "Error: prometheus-cpp was not found. Please install prometheus-cpp. \n\t\t brew install prometheus-cpp https://github.com/jupp0r/prometheus-cpp")
  endif()
endif()

if(NOT DEFINED prometheus-cpp_VERSION)
  message(FATAL_ERROR "prometheus-cpp_VERSION is not defined")
endif()


# openssl and crypto libs
IF(CRYPTO_CUSTOM_LIBRARY) 
  SET(RDRS_CRYPTO_LIBRARY ${CRYPTO_CUSTOM_LIBRARY})
ELSE()
  SET(RDRS_CRYPTO_LIBRARY ${CRYPTO_LIBRARY})
ENDIF()

IF(OPENSSL_CUSTOM_LIBRARY) 
  SET(RDRS_OPENSSL_LIBRARY ${OPENSSL_CUSTOM_LIBRARY})
ELSE()
  SET(RDRS_OPENSSL_LIBRARY ${OPENSSL_LIBRARY})
ENDIF()

if(OPENSSL_INCLUDE_DIR)
  MESSAGE(STATUS "RDRS Server Using OpenSSL Configured by RonDB") 
  MESSAGE(STATUS "RDRS Server OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR}") 
  MESSAGE(STATUS "RDRS Server RDRS_OPENSSL_LIBRARY ${RDRS_OPENSSL_LIBRARY}") 
  MESSAGE(STATUS "RDRS Server RDRS_CRYPTO_LIBRARY ${RDRS_CRYPTO_LIBRARY}") 
  INCLUDE_DIRECTORIES(${OPENSSL_INCLUDE_DIR})
else()
  message(FATAL_ERROR "Requested OpenSSL TLS provider but OpenSSL was not found")
endif()

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include)
INCLUDE_DIRECTORIES(${RDRS_DROGON_INCLUDE_DIR})
#INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_DIR})
INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_INCLUDE_DIR})
LINK_DIRECTORIES(${RDRS_DROGON_LIB_DIR})
INCLUDE_DIRECTORIES(${AVROCPP_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(${PROJECT_NAME}
                      base64
                      ${AVROCPP_LIBRARIES}
                      Jsoncpp_lib
                      Drogon::Drogon
                      Trantor::Trantor
                      simdjson::simdjson
                      ${RDRS_OPENSSL_LIBRARY}
                      ${RDRS_CRYPTO_LIBRARY}
                      prometheus-cpp::push
                      ndbgeneral
                      rdrs_string
                      mysqlclient
                      ronsql)

INSTALL(TARGETS ${PROJECT_NAME}
    DESTINATION ${INSTALL_BINDIR} COMPONENT Server)

add_library(rdrs2_lib STATIC ${SRC})

target_include_directories(rdrs2_lib PUBLIC 
	${CMAKE_CURRENT_SOURCE_DIR} 
	${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include)
target_include_directories(rdrs2_lib PUBLIC ${RDRS_DROGON_INCLUDE_DIR})
target_include_directories(rdrs2_lib PUBLIC ${RDRS_SIMDJSON_INCLUDE_DIR})
target_include_directories(rdrs2_lib PUBLIC ${RDRS_AVRO_INCLUDE_DIR})
target_link_directories(rdrs2_lib PUBLIC ${RDRS_DROGON_LIB_DIR})

# Install avrocpp library
INSTALL(FILES ${AVROCPP_LIBRARIES} DESTINATION lib)
EXECUTE_PROCESS(
      COMMAND readlink -f "${AVROCPP_LIBRARIES}" OUTPUT_VARIABLE AVROCPP_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
INSTALL(FILES ${AVROCPP_VERSION} DESTINATION lib)

# Install jsoncpp library
EXECUTE_PROCESS(
       COMMAND readlink "${JSONCPP_LIBRARIES}" OUTPUT_VARIABLE JSONCPP_VERSION_LINK
      OUTPUT_STRIP_TRAILING_WHITESPACE)
EXECUTE_PROCESS(
       COMMAND dirname "${JSONCPP_LIBRARIES}" OUTPUT_VARIABLE JSONCPP_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
EXECUTE_PROCESS(
       COMMAND readlink -f "${JSONCPP_LIBRARIES}" OUTPUT_VARIABLE JSONCPP_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
INSTALL(FILES ${JSONCPP_LIBRARIES} DESTINATION lib)
INSTALL(FILES ${JSONCPP_VERSION_DIR}/${JSONCPP_VERSION_LINK} DESTINATION lib)
INSTALL(FILES ${JSONCPP_VERSION} DESTINATION lib)

# Install prometheus-cpp libraries
get_target_property(lib_core_path prometheus-cpp::core IMPORTED_LOCATION_NOCONFIG)
get_target_property(lib_push_path prometheus-cpp::push IMPORTED_LOCATION_NOCONFIG)
INSTALL(FILES ${lib_core_path} DESTINATION lib)
INSTALL(FILES ${lib_push_path} DESTINATION lib)
EXECUTE_PROCESS(
       COMMAND dirname "${lib_core_path}" OUTPUT_VARIABLE PROMETHEUS_CPP_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
INSTALL(FILES "${PROMETHEUS_CPP_DIR}/libprometheus-cpp-core.so.1.3" DESTINATION lib)
INSTALL(FILES "${PROMETHEUS_CPP_DIR}/libprometheus-cpp-push.so.1.3" DESTINATION lib)

# Install uuid library
if(UUID_LIBRARY)
    message(STATUS "Found uuid_library: ${UUID_LIBRARY}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${UUID_LIBRARY}" OUTPUT_VARIABLE UUID_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${UUID_LIBRARY}" OUTPUT_VARIABLE UUID_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${UUID_VERSION} DESTINATION lib)
    INSTALL(FILES "${UUID_VERSION_DIR}/libuuid.so.1" DESTINATION lib)
endif()

# Install ossp-uuid library
if(OSSP_UUID_LIBRARY)
    message(STATUS "Found ossp_uuid_library: ${OSSP_UUID_LIBRARY}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${OSSP_UUID_LIBRARY}" OUTPUT_VARIABLE OSSP_UUID_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${OSSP_UUID_LIBRARY}" OUTPUT_VARIABLE OSSP_UUID_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${OSSP_UUID_VERSION} DESTINATION lib)
    INSTALL(FILES "${OSSP_UUID_VERSION_DIR}/libossp-uuid.so.16" DESTINATION lib)
endif()

# Install boost_filesystem library
if(BOOST_FS)
    message(STATUS "Found boost_filesystem_library: ${BOOST_FS}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_FS}" OUTPUT_VARIABLE BOOST_FS_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_FS}" OUTPUT_VARIABLE BOOST_FS_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_FS_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_FS_VERSION_DIR}/libboost_filesystem.so" DESTINATION lib)
endif()

# Install boost_iostreams library
if(BOOST_IO)
    message(STATUS "Found boost_iostreams_library: ${BOOST_IO}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_IO}" OUTPUT_VARIABLE BOOST_IO_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_IO}" OUTPUT_VARIABLE BOOST_IO_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_IO_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_IO_VERSION_DIR}/libboost_iostreams.so" DESTINATION lib)
endif()

# Install boost_program_options library
if(BOOST_PO)
    message(STATUS "Found boost_program_options_library: ${BOOST_PO}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_PO}" OUTPUT_VARIABLE BOOST_PO_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_PO}" OUTPUT_VARIABLE BOOST_PO_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_PO_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_PO_VERSION_DIR}/libboost_program_options.so" DESTINATION lib)
endif()

# Install boost_regex library
if(BOOST_RE)
    message(STATUS "Found boost_regex_library: ${BOOST_RE}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_RE}" OUTPUT_VARIABLE BOOST_RE_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_RE}" OUTPUT_VARIABLE BOOST_RE_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_RE_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_RE_VERSION_DIR}/libboost_regex.so" DESTINATION lib)
endif()

# Install boost_system library
if(BOOST_SY)
    message(STATUS "Found boost_system_library: ${BOOST_SY}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_SY}" OUTPUT_VARIABLE BOOST_SY_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_SY}" OUTPUT_VARIABLE BOOST_SY_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_SY_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_SY_VERSION_DIR}/libboost_system.so" DESTINATION lib)
endif()

# Install boost_atomic library
if(BOOST_AT)
    message(STATUS "Found boost_atomic_library: ${BOOST_AT}")
    EXECUTE_PROCESS(
       COMMAND readlink -f "${BOOST_AT}" OUTPUT_VARIABLE BOOST_AT_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
       COMMAND dirname "${BOOST_AT}" OUTPUT_VARIABLE BOOST_AT_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${BOOST_AT_VERSION} DESTINATION lib)
    INSTALL(FILES "${BOOST_AT_VERSION_DIR}/libboost_atomic.so" DESTINATION lib)
endif()

# todo-ronsql is ronsql needed for rdrs2_lib? It's used in ../test/api_key_test
TARGET_LINK_LIBRARIES(rdrs2_lib
                      base64
                      ${AVROCPP_LIBRARIES}
                      Jsoncpp_lib
                      Drogon::Drogon
                      Trantor::Trantor
                      simdjson::simdjson
                      ${RDRS_OPENSSL_LIBRARY}
                      ${RDRS_CRYPTO_LIBRARY}
                      prometheus-cpp::push
                      ndbclient_so
                      ndbgeneral
                      rdrs_string
                      mysqlclient
                      ronsql)
