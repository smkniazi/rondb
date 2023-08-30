project (server LANGUAGES CXX C VERSION 1.0.0)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} /Users/salman/code/hops/rondb/extra/drogon/drogon-1.8.4/cmake_modules)

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Jsoncpp REQUIRED)
find_package(Threads REQUIRED)

INCLUDE_DIRECTORIES(${SIMDJSON_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DROGON_INCLUDE_DIR} ${DROGON_ORM_INCLUDE_DIR} ${DROGON_INCLUDE_DIR_BUILD} ${DROGON_NOSQL_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${TRANTOR_INCLUDE_DIR})
INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})

file(GLOB SRC
	*.cc
	*.c
	*.cpp
	*.h
)

# -g for debug, -03 for release
# target_compile_options(${PROJECT_NAME} PUBLIC -O3)
add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} PRIVATE Jsoncpp_lib ${DROGON_LIBRARY} ${SIMDJSON_LIBRARY} ${TRANTOR_LIBRARY})

