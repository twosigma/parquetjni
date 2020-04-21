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

#include "com_twosigma_parquetjni_ParquetJni.h"

#include <arrow/buffer.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/writer.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <jni.h>
#include <parquet/api/reader.h>
#include <parquet/statistics.h>

#include <mutex>

#include "parquetjni/memory_tracking.h"
#include "parquetjni/parquetjni.h"
#include "parquetjni/s3file.h"
#include "parquetjni/util.h"

// Ensure that we initialize glog exactly once
std::once_flag log_handle;

// JNI initialization callback
jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  (void)vm;
  (void)reserved;

  std::call_once(log_handle, []() { google::InitGoogleLogging("ParquetJNI"); });

  return JNI_VERSION_1_6;
}

// Helper to throw an IOException on the Java side.
jint ThrowIoException(JNIEnv *env, std::string message) {
  jclass exc_class = env->FindClass("java/io/IOException");
  return env->ThrowNew(exc_class, message.c_str());
}

// Helper to convert a jstring into a std::string.
std::string string_of_jstring(JNIEnv *env, jstring s) {
  std::string result = "";
  if (s) {
    jboolean is_copy;
    const char *converted = env->GetStringUTFChars(s, &is_copy);
    result = std::string(converted, env->GetStringLength(s));
    env->ReleaseStringUTFChars(s, converted);
  }
  return result;
}

// Helper to hold a std::shared_ptr from Java.
struct RecordBatchReaderJniHandle {
  std::shared_ptr<arrow::RecordBatchReader> reader;
};

JNIEXPORT jlong JNICALL
Java_com_twosigma_parquetjni_ParquetJni_allocate__Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2(
    JNIEnv *env, jclass this_class, jstring endpoint, jstring bucket,
    jstring key, jstring auth_token) {
  (void)this_class;
  ParquetJniReader *reader = nullptr;
  const auto profile_start = std::chrono::steady_clock::now();
  const arrow::Status result = SingleFileParquetJniReader::Open(
      string_of_jstring(env, endpoint), string_of_jstring(env, bucket),
      string_of_jstring(env, key), string_of_jstring(env, auth_token), &reader);
  if (!result.ok()) {
    ThrowIoException(env, result.ToString());
    return 0;
  }
  TRACE("ParquetJniReader::Open", profile_start, open_end);
  return reinterpret_cast<uintptr_t>(reader);
}

JNIEXPORT jlong JNICALL
Java_com_twosigma_parquetjni_ParquetJni_allocate__Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2_3Ljava_lang_String_2(
    JNIEnv *env, jclass this_class, jstring endpoint, jstring auth_token,
    jstring bucket, jobjectArray keys) {
  (void)this_class;
  const auto profile_start = std::chrono::steady_clock::now();
  if (!keys) {
    ThrowIoException(env, "Keys must be provided");
    return 0;
  }
  std::vector<std::string> paths;
  int string_count = env->GetArrayLength(keys);
  for (int i = 0; i < string_count; i++) {
    jstring s = static_cast<jstring>(env->GetObjectArrayElement(keys, i));
    paths.push_back(string_of_jstring(env, s));
  }

  std::shared_ptr<ParquetJniReader> shared_reader;
  const arrow::Status result = ParquetJniDataset::Open(
      string_of_jstring(env, endpoint), string_of_jstring(env, auth_token),
      string_of_jstring(env, bucket), paths, &shared_reader);
  if (!result.ok()) {
    ThrowIoException(env, result.ToString());
    return 0;
  }
  ParquetJniReader *reader = new SharedPtrHolder(std::move(shared_reader));
  TRACE("ParquetJniDataset::Open", profile_start, open_end);
  return reinterpret_cast<uintptr_t>(reader);
}

JNIEXPORT jlong JNICALL
Java_com_twosigma_parquetjni_ParquetJni_allocate__Ljava_lang_String_2(
    JNIEnv *env, jclass this_class, jstring file_path) {
  (void)this_class;
  ParquetJniReader *reader = nullptr;
  const arrow::Status result = SingleFileParquetJniReader::Open(
      string_of_jstring(env, file_path), &reader);
  if (!result.ok()) {
    ThrowIoException(env, result.ToString());
    return 0;
  }
  return reinterpret_cast<uintptr_t>(reader);
}

JNIEXPORT jlong JNICALL
Java_com_twosigma_parquetjni_ParquetJni_getRecordBatchReader(
    JNIEnv *env, jclass this_class, jlong reader_ref, jobjectArray columns,
    jstring time_column, jlong min_utc_ns, jlong max_utc_ns,
    jboolean filter_row_groups) {
  (void)this_class;
  ParquetJniReader *reader = reinterpret_cast<ParquetJniReader *>(reader_ref);
  if (!reader) {
    ThrowIoException(env, "Null reader");
    return 0;
  }

  RecordBatchReaderJniHandle *result = new RecordBatchReaderJniHandle();
  std::string time_column_cpp = string_of_jstring(env, time_column);

  std::vector<std::string> selected_columns;
  if (columns) {
    int string_count = env->GetArrayLength(columns);
    for (int i = 0; i < string_count; i++) {
      jstring s = static_cast<jstring>(env->GetObjectArrayElement(columns, i));
      selected_columns.push_back(string_of_jstring(env, s));
    }
  }

  const arrow::Status &status = reader->GetRecordBatchReader(
      selected_columns, time_column_cpp, min_utc_ns, max_utc_ns,
      filter_row_groups, &result->reader);
  if (!status.ok()) {
    ThrowIoException(env, status.ToString());
    return 0;
  }

  if (!result->reader) {
    return 0;
  }

  return reinterpret_cast<uintptr_t>(result);
}

JNIEXPORT jbyteArray JNICALL
Java_com_twosigma_parquetjni_ParquetJni_readReaderSchema(JNIEnv *env,
                                                         jclass this_class,
                                                         jlong handle_ref) {
  (void)this_class;
  RecordBatchReaderJniHandle *handle =
      reinterpret_cast<RecordBatchReaderJniHandle *>(handle_ref);
  if (!handle) {
    ThrowIoException(env, "Null reader");
    return nullptr;
  }

  const auto profile_start = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Schema> schema = handle->reader->schema();
  std::shared_ptr<arrow::Buffer> buf;
  arrow::ipc::SerializeSchema(*schema, nullptr, GetTrackedPool(), &buf);

  jbyteArray serialized =
      static_cast<jbyteArray>(env->NewByteArray(buf->size()));
  // TODO(lidavidm): check for null
  env->SetByteArrayRegion(serialized, 0, buf->size(),
                          reinterpret_cast<const jbyte *>(buf->data()));
  TRACE("RecordBatchReader::GetSchema", profile_start, schema_end);
  return serialized;
}

JNIEXPORT void JNICALL
Java_com_twosigma_parquetjni_ParquetJni_closeRecordBatchReader(
    JNIEnv *env, jclass this_class, jlong handle_ref) {
  (void)this_class;
  RecordBatchReaderJniHandle *handle =
      reinterpret_cast<RecordBatchReaderJniHandle *>(handle_ref);
  if (!handle) {
    ThrowIoException(env, "Null reader");
    return;
  }

  delete handle;
}

JNIEXPORT jbyteArray JNICALL
Java_com_twosigma_parquetjni_ParquetJni_nextRecordBatch(JNIEnv *env,
                                                        jclass this_class,
                                                        jlong handle_ref) {
  (void)this_class;
  RecordBatchReaderJniHandle *handle =
      reinterpret_cast<RecordBatchReaderJniHandle *>(handle_ref);
  if (!handle) {
    ThrowIoException(env, "Null reader");
    return 0;
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  // Don't directly trace this as it's internally traced
  const arrow::Status &status = handle->reader->ReadNext(&batch);
  if (!status.ok()) {
    ThrowIoException(env, status.ToString());
    return nullptr;
  }
  if (batch == nullptr) {
    return nullptr;
  }

  const auto profile_start = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Buffer> buf;
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.memory_pool = GetTrackedPool();
  arrow::ipc::SerializeRecordBatch(*batch, options, &buf);
  TRACE("RecordBatchReader::SerializeRecordBatch", profile_start,
        serialize_end);

  jbyteArray serialized =
      static_cast<jbyteArray>(env->NewByteArray(buf->size()));
  // TODO(lidavidm): check for null
  env->SetByteArrayRegion(serialized, 0, buf->size(),
                          reinterpret_cast<const jbyte *>(buf->data()));
  TRACE("RecordBatchReader::SetByteArrayRegion", serialize_end, copy_end);
  return serialized;
}

JNIEXPORT void JNICALL Java_com_twosigma_parquetjni_ParquetJni_closeJni(
    JNIEnv *env, jclass this_class, jlong reader_ref) {
  (void)env;
  (void)this_class;
  ParquetJniReader *reader = reinterpret_cast<ParquetJniReader *>(reader_ref);
  if (!reader) {
    return;
  }
  const arrow::Status &status = reader->Close();
  if (!status.ok()) {
    ThrowIoException(env, status.ToString());
    // Free the reader even if closing fails
  }
  delete reader;
}

JNIEXPORT jlong JNICALL Java_com_twosigma_parquetjni_ParquetJni_maxAllocation(
    JNIEnv *env, jclass this_class) {
  (void)env;
  (void)this_class;
  return arrow::default_memory_pool()->max_memory();
}

JNIEXPORT jlong JNICALL
Java_com_twosigma_parquetjni_ParquetJni_maxTrackedAllocation(
    JNIEnv *env, jclass this_class) {
  (void)env;
  (void)this_class;
  return GetTrackedPool()->max_memory();
}

JNIEXPORT void JNICALL
Java_com_twosigma_parquetjni_ParquetJni_resetTrackedAllocation(
    JNIEnv *env, jclass this_class) {
  (void)env;
  (void)this_class;
  ResetTrackedPool();
}
