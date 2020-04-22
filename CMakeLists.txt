# | Copyright 2020 Two Sigma Investments, LP.
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     https://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

cmake_minimum_required(VERSION 3.15)
project(
  parquetjni
  VERSION 0.1
  DESCRIPTION "JNI bindings to the Arrow/C++ Parquet library"
  LANGUAGES CXX
  )

find_package(Java 1.8 COMPONENTS Development)
find_package(PkgConfig REQUIRED)
include(UseJava)

find_package(Arrow CONFIG REQUIRED)

# Work around FindJNI not finding this in some cases
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI REQUIRED)
find_package(Parquet REQUIRED)

get_target_property(ARROW_INCLUDE_DIRS arrow_shared INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(PARQUET_INCLUDE_DIRS parquet_shared INTERFACE_INCLUDE_DIRECTORIES)

message(STATUS "Dependencies configured via CMake:")
message(STATUS "Arrow version: ${Arrow_VERSION}")
message(STATUS "Arrow includes: ${ARROW_INCLUDE_DIRS}")
message(STATUS "Parquet version: ${Parquet_VERSION}")
message(STATUS "Parquet includes: ${PARQUET_INCLUDE_DIRS}")
message(STATUS "JNI headers: ${JNI_INCLUDE_DIRS}")

message(STATUS "Dependencies configured via manually-set variables:")
message(STATUS "(set these via cmake -D...=...)")
message(STATUS "AWSSDK_INCLUDE_DIRS: ${AWSSDK_INCLUDE_DIRS}")
message(STATUS "AWSSDK_LINK_DIRS: ${AWSSDK_LINK_DIRS}")
message(STATUS "GLOG_INCLUDE_DIRS: ${GLOG_INCLUDE_DIRS}")
message(STATUS "GLOG_LINK_DIRS: ${GLOG_LINK_DIRS}")

set(GENERATED_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/parquetjni_generated/")
message(STATUS "Generating headers to ${GENERATED_HEADERS_DIR}")

add_compile_options(-Wall -Werror)
link_directories(${AWSSDK_LINK_DIRS} ${GLOG_LINK_DIRS})

# Create a dummy target to generate the JNI headers.
add_jar(
  parquetjni_headers
  src/main/java/com/twosigma/parquetjni/ParquetJni.java
  GENERATE_NATIVE_HEADERS parquetjni
  DESTINATION ${GENERATED_HEADERS_DIR}
  )

add_library(
  parquetjni_lib
  SHARED
  cpp/parquetjni/com_twosigma_parquetjni_ParquetJni.cc
  cpp/parquetjni/memory_tracking.cc
  cpp/parquetjni/parquetjni.cc
  cpp/parquetjni/s3file.cc
  )
add_dependencies(parquetjni_lib parquetjni_headers)
set_property(TARGET parquetjni_lib PROPERTY CXX_STANDARD 11)

target_include_directories(
  parquetjni_lib
  PRIVATE
  cpp/
  ${GENERATED_HEADERS_DIR}
  ${ARROW_INCLUDE_DIRS}
  ${AWSSDK_INCLUDE_DIRS}
  ${GLOG_INCLUDE_DIRS}
  ${JNI_INCLUDE_DIRS}
  ${PARQUET_INCLUDE_DIRS}
  )

target_link_libraries(parquetjni_lib PUBLIC arrow_shared parquet_shared glog aws-cpp-sdk-s3)

install(
  TARGETS parquetjni_lib
  DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
  )
