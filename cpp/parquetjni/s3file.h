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

#ifndef PARQUETJNI_S3FILE_H_
#define PARQUETJNI_S3FILE_H_

#include <arrow/api.h>
#include <arrow/io/interfaces.h>

#include <memory>
#include <string>

/**
 * This is a backported version of Apache Arrow S3FileSystem code from
 * their master branch, adapted to work as a minimal proof-of-concept
 * within the TS environment.
 */

struct S3Path {
  std::string bucket;
  std::string key;
};

arrow::Status OpenS3File(const S3Path& path, const std::string& endpoint,
                         const std::string& access_token,
                         std::shared_ptr<arrow::io::RandomAccessFile>* out);

#endif  // PARQUETJNI_S3FILE_H_
