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
#include <future>
#include <queue>
#include <thread>
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
  return Open(file_path, arrow_file, out);
}

arrow::Status SingleFileParquetJniReader::Open(const std::string &endpoint,
                                               const std::string &bucket,
                                               const std::string &key,
                                               const std::string &auth_token,
                                               ParquetJniReader **out) {
  S3Path path{bucket, key};
  std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
  ARROW_RETURN_NOT_OK(OpenS3File(path, endpoint, auth_token, &arrow_file));
  return Open(key, arrow_file, out);
}

arrow::Status SingleFileParquetJniReader::Open(
    std::string name, std::shared_ptr<arrow::io::RandomAccessFile> arrow_file,
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
  *out =
      new SingleFileParquetJniReader(std::move(name), std::move(arrow_reader));
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
  TRACE_WITH("ParquetJniReader::ReadMetadata", name_, profile_start,
             metadata_end);
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
  TRACE_WITH("ParquetJniReader::FilterRowsColumns", name_, metadata_end,
             filter_end);

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
  TRACE_WITH("ParquetJniReader::OpenRecordBatchReader", name_, filter_end,
             open_end);
  return arrow::Status::OK();
}

arrow::Status SingleFileParquetJniReader::Close() {
  return arrow::Status::OK();
}

/// The maximum number of Parquet files to hold in memory at once.
constexpr int8_t kMaxFilesBuffered = 3;

/// State for a single Parquet file being buffered.
class BufferedFile {
 public:
  explicit BufferedFile(std::string name, arrow::Status status,
                        std::unique_ptr<ParquetJniReader> file,
                        std::shared_ptr<arrow::RecordBatchReader> reader)
      : name_(std::move(name)),
        status_(status),
        file_(std::move(file)),
        reader_(std::move(reader)) {}

  const std::string &name() const { return name_; }

  arrow::Status Close() {
    RETURN_NOT_OK(status_);
    reader_.reset();
    return file_->Close();
  }

  arrow::Status GetReader(
      std::shared_ptr<arrow::RecordBatchReader> *reader) const {
    RETURN_NOT_OK(status_);
    *reader = reader_;
    return arrow::Status::OK();
  }

 private:
  std::string name_;
  arrow::Status status_;
  // These pointers are only valid if status is Status::OK.
  std::unique_ptr<ParquetJniReader> file_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};

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
        schema_(nullptr),
        file_readers_(),
        file_queue_(std::deque<std::string>(dataset_->keys_.begin(),
                                            dataset_->keys_.end())) {}

  ~DatasetBatchReader() = default;

  static arrow::Status Open(std::shared_ptr<ParquetJniDataset> dataset,
                            const std::vector<std::string> &columns,
                            const std::string &time_column, int64_t min_utc_ns,
                            int64_t max_utc_ns, bool filter_row_groups,
                            std::shared_ptr<arrow::RecordBatchReader> *out) {
    const auto profile_start = std::chrono::steady_clock::now();
    *out = std::make_shared<DatasetBatchReader>(std::move(dataset), columns,
                                                time_column, min_utc_ns,
                                                max_utc_ns, filter_row_groups);
    // TODO(lidavidm): use Datasets to do partition filtering here if needed

    // Access private fields
    DatasetBatchReader *reader = static_cast<DatasetBatchReader *>(out->get());
    // Load the first file and get the schema.
    ARROW_RETURN_NOT_OK(reader->EnqueueReads(kMaxFilesBuffered));
    if (reader->file_readers_.empty()) {
      return arrow::Status::Invalid("No files in dataset");
    }
    TRACE("ParquetJniDataset::Open::LoadNext", profile_start, load_end);
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    RETURN_NOT_OK(reader->file_readers_.front().get().GetReader(&batch_reader));
    reader->schema_ = batch_reader->schema();
    TRACE("ParquetJniDataset::Open::Schema", load_end, open_end);
    return arrow::Status::OK();
  }

  arrow::Status LoadNext() {
    if (!file_readers_.empty()) {
      BufferedFile &buffered_file =
          const_cast<BufferedFile &>(file_readers_.front().get());
      RETURN_NOT_OK(buffered_file.Close());
      file_readers_.pop();
    }
    return EnqueueReads(kMaxFilesBuffered);
  }

  arrow::Status EnqueueReads(size_t max_files) {
    const auto profile_start = std::chrono::steady_clock::now();
    while (file_readers_.size() < max_files && !file_queue_.empty()) {
      const std::string next_file = file_queue_.front();
      file_queue_.pop();
      std::packaged_task<BufferedFile()> task(
          [next_file, this, profile_start]() {
            ParquetJniReader *jni_reader;
            arrow::Status status = SingleFileParquetJniReader::Open(
                dataset_->endpoint_, dataset_->bucket_, next_file,
                dataset_->auth_token_, &jni_reader);
            if (!status.ok()) {
              return BufferedFile(next_file, status, nullptr, nullptr);
            }
            std::shared_ptr<arrow::RecordBatchReader> batch_reader;
            status = jni_reader->GetRecordBatchReader(
                columns_, time_column_, min_utc_ns_, max_utc_ns_,
                filter_row_groups_, &batch_reader);
            if (!status.ok()) {
              return BufferedFile(next_file, status, nullptr, nullptr);
            }
            TRACE_WITH("ParquetJniDataset::EnqueueReads", next_file,
                       profile_start, load_end);
            return BufferedFile(next_file, arrow::Status::OK(),
                                std::unique_ptr<ParquetJniReader>(jni_reader),
                                std::move(batch_reader));
          });
      std::future<BufferedFile> future = task.get_future();
      std::thread thread(std::move(task));
      thread.detach();
      file_readers_.emplace(std::move(future));
    }
    TRACE("ParquetJniDataset::EnqueueReads", profile_start, load_end);
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
    const auto profile_start = std::chrono::steady_clock::now();
    ARROW_RETURN_NOT_OK(EnqueueReads(kMaxFilesBuffered));
    TRACE("ParquetJniDataset::ReadNext::EnqueueReads", profile_start,
          enqueue_end);
    if (file_readers_.empty()) {
      *batch = nullptr;
      return arrow::Status::OK();
    }
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    const BufferedFile &file = file_readers_.front().get();
    TRACE_WITH("ParquetJniDataset::ReadNext::GetFront", file.name(),
               enqueue_end, get_end);
    ARROW_RETURN_NOT_OK(file.GetReader(&batch_reader));
    TRACE_WITH("ParquetJniDataset::ReadNext::GetReader", file.name(), get_end,
               reader_end);
    ARROW_RETURN_NOT_OK(batch_reader->ReadNext(batch));
    TRACE_WITH("ParquetJniDataset::ReadNext::ReadNext", file.name(), reader_end,
               reading_end);
    if (batch == nullptr || *batch == nullptr) {
      // This file was drained, load a new file
      ARROW_RETURN_NOT_OK(LoadNext());
      TRACE_WITH("ParquetJniDataset::ReadNext (EOF)", file.name(), reading_end,
                 read_end);
      return ReadNext(batch);
    }
    TRACE_WITH("ParquetJniDataset::ReadNext", file.name(), reading_end,
               read_end);
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
  // Queue of files buffered for reading. The head of the queue is the
  // file currently being read.
  std::queue<std::shared_future<BufferedFile>> file_readers_;
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
