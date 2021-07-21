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

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "/usr/local/lib/cmake/arrow")

# This ensures that things like gnu++11 get passed correctly
# c++20 is for std::chrono::sys_days
set(CMAKE_CXX_STANDARD 20)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Werror)

# Look for installed packages the system
find_package(gflags REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
# From https://github.com/apache/arrow/blob/afe5515754d4c4f67a0e5ee066513f8383f00d70/cpp/examples/minimal_build/CMakeLists.txt#L37
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR} ${GFLAGS_INCLUDE_DIRS})

set(COMMON_LIBS Threads::Threads parquet_static arrow_static)

add_executable(parquet-diff src/parquet-diff.cc src/common.cc)
target_link_libraries(parquet-diff PRIVATE -static ${COMMON_LIBS})

add_executable(parquet-to-arrow src/parquet-to-arrow.cc src/common.cc)
target_link_libraries(parquet-to-arrow PRIVATE -static ${COMMON_LIBS})

add_executable(parquet-to-text-stream src/parquet-to-text-stream.cc src/common.cc src/range.cc)
target_link_libraries(parquet-to-text-stream PRIVATE -static -lgflags ${COMMON_LIBS})

install(TARGETS parquet-diff parquet-to-arrow parquet-to-text-stream DESTINATION /usr/bin)
