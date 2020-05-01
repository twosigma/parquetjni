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

/** Basic unit tests. */

#include "parquetjni/parquetjni.h"

#include <arrow/record_batch.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>

#include "parquetjni/util.h"

#define ASSERT_OK(expr)                       \
  do {                                        \
    auto __s = (expr);                        \
    if (!__s.ok()) {                          \
      FAIL() << "Failed: " << __s.ToString(); \
    }                                         \
  } while (false)

constexpr char kEndpointEnv[] = "PARQUETJNI_TEST_S3_ENDPOINT";
constexpr char kBucketEnv[] = "PARQUETJNI_TEST_BUCKET";
constexpr char kDefaultBucket[] = "bucket";

TEST(ParquetJniDataset, ScanDataset) {
  const char* c_endpoint = std::getenv(kEndpointEnv);
  if (!c_endpoint) {
    FAIL() << "Must specify endpoint with " << kEndpointEnv;
  }

  std::string endpoint = c_endpoint;
  std::string bucket = kDefaultBucket;
  const char* c_bucket = std::getenv(kBucketEnv);
  if (c_bucket) {
    bucket = c_bucket;
  }

  LOG(INFO) << "Testing endpoint " << endpoint << " and bucket " << bucket;

  const auto test_start = std::chrono::steady_clock::now();
  std::shared_ptr<ParquetJniReader> dataset;
  std::shared_ptr<arrow::RecordBatchReader> reader;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::vector<std::string> keys{"file_0.parquet", "file_1.parquet",
                                "file_2.parquet", "file_3.parquet"};
  ASSERT_OK(ParquetJniDataset::Open(endpoint, "", "bucket", keys, &dataset));
  ASSERT_OK(dataset->GetRecordBatchReader({}, "", 0, 0, false, &reader));
  do {
    const auto profile_start = std::chrono::steady_clock::now();
    ASSERT_OK(reader->ReadNext(&batch));
    TRACE("UnitTest::ReadBatch", profile_start, read_end);
  } while (batch);
  ASSERT_OK(dataset->Close());
  TRACE("UnitTest", test_start, test_end);
}
