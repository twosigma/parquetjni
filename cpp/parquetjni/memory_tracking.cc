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

#include "parquetjni/memory_tracking.h"

// An Arrow memory pool with allocation tracking enabled.
static arrow::MemoryPool *kTrackedPool =
    new arrow::ProxyMemoryPool(arrow::default_memory_pool());

arrow::MemoryPool *GetTrackedPool() {
  // TODO(lidavidm): synchronization
  return kTrackedPool;
}

void ResetTrackedPool() {
  // TODO(lidavidm): use our own wrapper that lets us reset statistics at will
  arrow::MemoryPool *old = kTrackedPool;
  kTrackedPool = new arrow::ProxyMemoryPool(arrow::default_memory_pool());
  delete old;
}
