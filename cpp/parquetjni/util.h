/*
 * Copyright 2020 Two Sigma Investments, LP.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PARQUETJNI_UTIL_H_
#define PARQUETJNI_UTIL_H_

#include <arrow/status.h>
#include <glog/logging.h>

#include <chrono>
#include <iomanip>
#include <thread>

// Abort if the status is an error.
#define PARQUETJNI_CHECK_OK(s)            \
  do {                                    \
    arrow::Status __result = (s);         \
    if (!__result.ok()) {                 \
      std::cerr << __result << std::endl; \
      std::abort();                       \
    }                                     \
  } while (false)

// Log a timing trace.
#define TRACE(LABEL, START, END)                                         \
  const auto END = std::chrono::steady_clock::now();                     \
  {                                                                      \
    const auto __start = (START);                                        \
    VLOG(2) << "TRACE: " << (LABEL) << ' ' << std::this_thread::get_id() \
            << ' ' << std::fixed << std::setprecision(5)                 \
            << __start.time_since_epoch().count() << ' '                 \
            << END.time_since_epoch().count() << std::endl;              \
  }

#define TRACE_WITH(LABEL, SPAN, START, END)                               \
  const auto END = std::chrono::steady_clock::now();                      \
  {                                                                       \
    const auto __start = (START);                                         \
    std::chrono::duration<double> __diff = END - __start;                 \
    VLOG(2) << "TRACE: " << (LABEL) << " label=" << (SPAN) << ' '         \
            << std::this_thread::get_id() << ' ' << std::fixed            \
            << std::setprecision(5) << __start.time_since_epoch().count() \
            << ' ' << END.time_since_epoch().count() << std::endl;        \
  }

#endif  // PARQUETJNI_UTIL_H_
