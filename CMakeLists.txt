# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

project(parquet-to-arrow)

include(FindPkgConfig)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

add_compile_options(-Wall -Werror)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)
# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Look for installed packages the system
pkg_search_module(ARROW REQUIRED arrow)
pkg_search_module(PARQUET REQUIRED parquet)
find_library(THRIFT NAMES libthrift.a libthriftd.a)
pkg_search_module(JEMALLOC REQUIRED jemalloc)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})

add_executable(parquet-to-arrow src/parquet-to-arrow.cc src/common.cc)
target_link_libraries(parquet-to-arrow ${PARQUET_STATIC_LDFLAGS} ${ARROW_STATIC_LDFLAGS} ${THRIFT} ${JEMALLOC_STATIC_LDFLAGS} -static -lpthread -lsnappy -lboost_regex -lboost_filesystem -lboost_system)

add_executable(parquet-to-text-stream src/parquet-to-text-stream.cc src/common.cc)
target_link_libraries(parquet-to-text-stream ${PARQUET_STATIC_LDFLAGS} ${ARROW_STATIC_LDFLAGS} ${THRIFT} ${JEMALLOC_STATIC_LDFLAGS} -static -lpthread -lsnappy -lboost_regex -lboost_filesystem -lboost_system -ldouble-conversion)
