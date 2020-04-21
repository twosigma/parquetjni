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

#ifndef PARQUETJNI_MEMORY_TRACKING_H_
#define PARQUETJNI_MEMORY_TRACKING_H_

#include <arrow/memory_pool.h>

/// Get a wrapped MemoryPool for tracking allocations.
arrow::MemoryPool *GetTrackedPool();

/// Replace the existing MemoryPool to reset its statistics.
void ResetTrackedPool();

#endif  // PARQUETJNI_MEMORY_TRACKING_H_
