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

#define PARQUETJNI_PROFILE

#include "parquetjni/parquetjni.h"

#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/options.h>
#include <arrow/table.h>
#include <parquet/api/reader.h>
#include <parquet/statistics.h>

#include <deque>
#include <queue>
#include <vector>

#include "parquetjni/memory_tracking.h"
#include "parquetjni/s3file.h"
#include "parquetjni/util.h"

/// The number of rows to feed through JNI at a time.
const int64_t kReaderChunkSize = 32768;

/// Wrap a TableBatchReader to keep the associated table alive.
class OwningTableBatchReader : public arrow::RecordBatchReader {
 public:
  explicit OwningTableBatchReader(std::shared_ptr<arrow::Table> table)
      : table_(table) {
    reader_ = std::make_shared<arrow::TableBatchReader>(*table_);
    reader_->set_chunksize(kReaderChunkSize);
  }
  ~OwningTableBatchReader() = default;

  std::shared_ptr<arrow::Schema> schema() const override {
    return reader_->schema();
  }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
    return reader_->ReadNext(batch);
  }

 private:
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<arrow::TableBatchReader> reader_;
};

arrow::Status SingleFileParquetJniReader::Open(const std::string &file_path,
                                               ParquetJniReader **out) {
  // Memory-mapped file
  ARROW_ASSIGN_OR_RAISE(
      auto arrow_file,
      arrow::io::MemoryMappedFile::Open(file_path, arrow::io::FileMode::READ));
  return Open(arrow_file, out);
}

arrow::Status SingleFileParquetJniReader::Open(const std::string &endpoint,
                                               const std::string &bucket,
                                               const std::string &key,
                                               const std::string &auth_token,
                                               ParquetJniReader **out) {
  S3Path path{bucket, key};
  std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
  ARROW_RETURN_NOT_OK(OpenS3File(path, endpoint, auth_token, &arrow_file));
  return Open(arrow_file, out);
}

arrow::Status SingleFileParquetJniReader::Open(
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file,
    ParquetJniReader **out) {
  const auto profile_start = std::chrono::steady_clock::now();
  // Enable parallel reads.
  parquet::ArrowReaderProperties properties;
  properties.set_use_threads(true);
  properties.set_pre_buffer(true);
  parquet::ReaderProperties parquet_properties =
      parquet::ReaderProperties(GetTrackedPool());
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  // TODO(lidavidm): measure overhead of this call; likely makes a
  // request to TS3
  parquet::arrow::FileReaderBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Open(std::move(arrow_file), parquet_properties));
  ARROW_RETURN_NOT_OK(builder.properties(properties)
                          ->memory_pool(GetTrackedPool())
                          ->Build(&arrow_reader));
  *out = new SingleFileParquetJniReader(std::move(arrow_reader));
  TRACE("ParquetJniReader::Open", profile_start, open_end);
  return arrow::Status::OK();
}

arrow::Status SingleFileParquetJniReader::GetSchema(
    std::shared_ptr<arrow::Schema> *out) {
  return arrow_reader_->GetSchema(out);
}

arrow::Status SingleFileParquetJniReader::GetRecordBatchReader(
    const std::vector<std::string> &columns, const std::string &time_column,
    const int64_t min_utc_ns, const int64_t max_utc_ns,
    const bool filter_row_groups,
    std::shared_ptr<arrow::RecordBatchReader> *out) {
  const parquet::ParquetFileReader *parquet_reader =
      arrow_reader_->parquet_reader();
  const bool has_column_filter = !columns.empty();
  std::vector<int> row_group_indices;
  std::vector<int> column_indices;
  const auto profile_start = std::chrono::steady_clock::now();
  std::shared_ptr<parquet::FileMetaData> metadata = parquet_reader->metadata();
  TRACE("ParquetJniReader::ReadMetadata", profile_start, metadata_end);
  for (int i = 0; i < arrow_reader_->num_row_groups(); i++) {
    bool include_row_group = true;
    if (time_column != "" || has_column_filter) {
      std::unique_ptr<parquet::RowGroupMetaData> row_metadata =
          metadata->RowGroup(i);
      for (int col_index = 0; col_index < row_metadata->num_columns();
           col_index++) {
        std::unique_ptr<parquet::ColumnChunkMetaData> col_metadata =
            row_metadata->ColumnChunk(col_index);

        // If a column list was supplied, check if the column is in
        // the list
        const auto &col_name = col_metadata->path_in_schema()->ToDotString();
        if (has_column_filter) {
          if (std::find(columns.begin(), columns.end(), col_name) !=
              columns.end()) {
            column_indices.push_back(col_index);
          }
        }

        // If a time column was supplied, check if the row group falls
        // in the given bounds
        if (col_name != time_column) {
          continue;
        }
        std::shared_ptr<parquet::Statistics> stats = col_metadata->statistics();
        if (stats->physical_type() != parquet::Type::type::INT64) {
          LOG(WARNING) << "Could not interpret physical type of time column";
          break;
        }
        const parquet::Int64Statistics *typed_stats =
            static_cast<const parquet::Int64Statistics *>(stats.get());
        // Skip row groups that don't fall in bounds
        include_row_group = (typed_stats->min() <= max_utc_ns) &&
                            (typed_stats->max() >= min_utc_ns);
        break;
      }
    }
    if (include_row_group) {
      row_group_indices.push_back(i);
    }
  }
  TRACE("ParquetJniReader::FilterRowsColumns", metadata_end, filter_end);

  if (row_group_indices.size() == 0) {
    // No data to read
    *out = nullptr;
    return arrow::Status::OK();
  }

  if (filter_row_groups && has_column_filter) {
    ARROW_RETURN_NOT_OK(arrow_reader_->GetRecordBatchReader(
        row_group_indices, column_indices, out));
  } else if (filter_row_groups) {
    ARROW_RETURN_NOT_OK(
        arrow_reader_->GetRecordBatchReader(row_group_indices, out));
  } else if (has_column_filter) {
    // Column list supplied
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader_->ReadTable(column_indices, &table));
    *out = std::make_shared<OwningTableBatchReader>(table);
  } else {
    // No column list supplied, read all columns
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader_->ReadTable(&table));
    *out = std::make_shared<OwningTableBatchReader>(table);
  }
  TRACE("ParquetJniReader::OpenRecordBatchReader", filter_end, open_end);
  return arrow::Status::OK();
}

arrow::Status SingleFileParquetJniReader::Close() {
  // TODO(lidavidm): figure out how to close parquet::arrow::FileReader
  // arrow_reader_->Close();
  return arrow::Status::OK();
}

class DatasetBatchReader : public arrow::RecordBatchReader {
 public:
  explicit DatasetBatchReader(std::shared_ptr<ParquetJniDataset> dataset,
                              const std::vector<std::string> &columns,
                              const std::string &time_column,
                              const int64_t min_utc_ns,
                              const int64_t max_utc_ns,
                              const bool filter_row_groups)
      : dataset_(std::move(dataset)),
        columns_(columns),
        time_column_(time_column),
        min_utc_ns_(min_utc_ns),
        max_utc_ns_(max_utc_ns),
        filter_row_groups_(filter_row_groups),
        file_queue_(std::deque<std::string>(dataset_->keys_.begin(),
                                            dataset_->keys_.end())) {}

  ~DatasetBatchReader() = default;

  static arrow::Status Open(std::shared_ptr<ParquetJniDataset> dataset,
                            const std::vector<std::string> &columns,
                            const std::string &time_column, int64_t min_utc_ns,
                            int64_t max_utc_ns, bool filter_row_groups,
                            std::shared_ptr<arrow::RecordBatchReader> *out) {
    *out = std::make_shared<DatasetBatchReader>(std::move(dataset), columns,
                                                time_column, min_utc_ns,
                                                max_utc_ns, filter_row_groups);
    // TODO(lidavidm): use Datasets to do partition filtering here if needed

    // Access private fields
    DatasetBatchReader *reader = static_cast<DatasetBatchReader *>(out->get());
    // Load the first file and get the schema.
    ARROW_RETURN_NOT_OK(reader->LoadNext());
    reader->schema_ = reader->current_reader_->schema();
    return arrow::Status::OK();
  }

  arrow::Status LoadNext() {
    current_reader_ = nullptr;
    if (current_jni_reader_) {
      RETURN_NOT_OK(current_jni_reader_->Close());
    }
    current_jni_reader_ = nullptr;
    if (file_queue_.empty()) {
      return arrow::Status::OK();
    }
    const std::string next_file = file_queue_.front();
    file_queue_.pop();
    ParquetJniReader *jni_reader;
    RETURN_NOT_OK(SingleFileParquetJniReader::Open(
        dataset_->endpoint_, dataset_->bucket_, next_file,
        dataset_->auth_token_, &jni_reader));
    current_jni_reader_.reset(jni_reader);
    const auto result = current_jni_reader_->GetRecordBatchReader(
        columns_, time_column_, min_utc_ns_, max_utc_ns_, filter_row_groups_,
        &current_reader_);
    return result;
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
    if (!current_reader_) {
      *batch = nullptr;
      return arrow::Status::OK();
    }
    ARROW_RETURN_NOT_OK(current_reader_->ReadNext(batch));
    if (batch == nullptr || *batch == nullptr) {
      // This file was drained, load a new file
      ARROW_RETURN_NOT_OK(LoadNext());
      return ReadNext(batch);
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<ParquetJniDataset> dataset_;
  std::vector<std::string> columns_;
  std::string time_column_;
  int64_t min_utc_ns_;
  int64_t max_utc_ns_;
  bool filter_row_groups_;

  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<ParquetJniReader> current_jni_reader_;
  std::shared_ptr<arrow::RecordBatchReader> current_reader_;
  std::queue<std::string> file_queue_;
};

arrow::Status ParquetJniDataset::Open(const std::string &endpoint,
                                      const std::string &auth_token,
                                      const std::string &bucket,
                                      const std::vector<std::string> &keys,
                                      std::shared_ptr<ParquetJniReader> *out) {
  *out = std::unique_ptr<ParquetJniDataset>(
      new ParquetJniDataset(endpoint, auth_token, bucket, keys));
  return arrow::Status::OK();
}

arrow::Status ParquetJniDataset::GetRecordBatchReader(
    const std::vector<std::string> &columns, const std::string &time_column,
    const int64_t min_utc_ns, const int64_t max_utc_ns,
    const bool filter_row_groups,
    std::shared_ptr<arrow::RecordBatchReader> *out) {
  return DatasetBatchReader::Open(shared_from_this(), columns, time_column,
                                  min_utc_ns, max_utc_ns, filter_row_groups,
                                  out);
}

arrow::Status ParquetJniDataset::Close() { return arrow::Status::OK(); }
