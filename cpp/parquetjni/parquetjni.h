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

#ifndef PARQUETJNI_PARQUETJNI_H_
#define PARQUETJNI_PARQUETJNI_H_

#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <jni.h>
#include <parquet/arrow/reader.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

extern "C" {
jint JNI_OnLoad(JavaVM *vm, void *reserved);
}

/// Base class for JNI wrappers around Arrow Parquet readers.
class ParquetJniReader {
 public:
  virtual ~ParquetJniReader() = default;

  /// \brief Get a stream of record batches.
  /// \param[in] columns Columns to select. If empty, read all columns.
  /// \param[in] time_column The name of the partition column. If empty,
  ///     do not filter row groups.
  /// \param[in] min_utc_ns The lower bound (inclusive) on the partition
  ///     column.
  /// \param[in] max_utc_ns The upper bound (exclusive) on the partition
  ///     column.
  /// \param[in] filter_row_groups If true, use Parquet metadata and the
  ///     partition column to filter which row groups to read.
  /// \param[out] out The record batch stream.
  virtual arrow::Status GetRecordBatchReader(
      const std::vector<std::string> &columns, const std::string &time_column,
      const int64_t min_utc_ns, const int64_t max_utc_ns,
      const bool filter_row_groups,
      std::shared_ptr<arrow::RecordBatchReader> *out) = 0;

  /// \brief Clean up the reader. Must be called prior to freeing an
  ///     instance of this class.
  virtual arrow::Status Close() = 0;
};

/**
 * A wrapper around an Arrow Parquet reader.
 */
class SingleFileParquetJniReader : public ParquetJniReader {
 public:
  /// \brief Open a local file for reading.
  static arrow::Status Open(const std::string &file_path,
                            ParquetJniReader **out);
  /// \brief Open an object on S3 for reading.
  /// \param[in] endpoint The S3 endpoint.
  /// \param[in] bucket The bucket.
  /// \param[in] key The key.
  /// \param[in] auth_token The OAuth token to use. If empty, use Kerberos.
  /// \param[out] out The created reader.
  static arrow::Status Open(const std::string &endpoint,
                            const std::string &bucket, const std::string &key,
                            const std::string &auth_token,
                            ParquetJniReader **out);
  static arrow::Status Open(
      std::shared_ptr<arrow::io::RandomAccessFile> arrow_file,
      ParquetJniReader **out);

  /// \brief Get the schema of the file.
  arrow::Status GetSchema(std::shared_ptr<arrow::Schema> *out);

  /// \brief Get a stream of record batches.
  /// \param[in] columns Columns to select. If empty, read all columns.
  /// \param[in] time_column The name of the partition column. If empty,
  ///     do not filter row groups.
  /// \param[in] min_utc_ns The lower bound (inclusive) on the partition
  ///     column.
  /// \param[in] max_utc_ns The upper bound (exclusive) on the partition
  ///     column.
  /// \param[in] filter_row_groups If true, use Parquet metadata and the
  ///     partition column to filter which row groups to read.
  /// \param[out] out The record batch stream.
  arrow::Status GetRecordBatchReader(
      const std::vector<std::string> &columns, const std::string &time_column,
      const int64_t min_utc_ns, const int64_t max_utc_ns,
      const bool filter_row_groups,
      std::shared_ptr<arrow::RecordBatchReader> *out) override;

  /// \brief Clean up the reader. Must be called prior to freeing an
  ///     instance of this class.
  arrow::Status Close() override;

 private:
  explicit SingleFileParquetJniReader(
      std::unique_ptr<parquet::arrow::FileReader> arrow_reader)
      : arrow_reader_(std::move(arrow_reader)) {}

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader_;
};

class DatasetBatchReader;
class ParquetJniDataset
    : public ParquetJniReader,
      public std::enable_shared_from_this<ParquetJniDataset> {
 public:
  static arrow::Status Open(const std::string &endpoint,
                            const std::string &auth_token,
                            const std::string &bucket,
                            const std::vector<std::string> &keys,
                            std::shared_ptr<ParquetJniReader> *out);

  arrow::Status GetRecordBatchReader(
      const std::vector<std::string> &columns, const std::string &time_column,
      const int64_t min_utc_ns, const int64_t max_utc_ns,
      const bool filter_row_groups,
      std::shared_ptr<arrow::RecordBatchReader> *out) override;

  arrow::Status Close() override;

 private:
  friend class DatasetBatchReader;
  explicit ParquetJniDataset(const std::string &endpoint,
                             const std::string &auth_token,
                             const std::string &bucket,
                             const std::vector<std::string> &keys)
      : endpoint_(endpoint),
        auth_token_(auth_token),
        bucket_(bucket),
        keys_(keys) {}

  // TODO(lidavidm): metadata cache should go here
  std::string endpoint_;
  std::string auth_token_;
  std::string bucket_;
  std::vector<std::string> keys_;
};

/// Keeps a reference to a shared_ptr alive.
class SharedPtrHolder : public ParquetJniReader {
 public:
  explicit SharedPtrHolder(std::shared_ptr<ParquetJniReader> delegate)
      : delegate_(std::move(delegate)) {}
  arrow::Status GetRecordBatchReader(
      const std::vector<std::string> &columns, const std::string &time_column,
      int64_t min_utc_ns, int64_t max_utc_ns, bool filter_row_groups,
      std::shared_ptr<arrow::RecordBatchReader> *out) override {
    return delegate_->GetRecordBatchReader(columns, time_column, min_utc_ns,
                                           max_utc_ns, filter_row_groups, out);
  }

  arrow::Status Close() override { return delegate_->Close(); }

 private:
  std::shared_ptr<ParquetJniReader> delegate_;
};

#endif  // PARQUETJNI_PARQUETJNI_H_
