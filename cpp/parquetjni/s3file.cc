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

/**
 * This is a backported version of Apache Arrow S3FileSystem code from
 * their master branch, adapted to work as a minimal proof-of-concept
 * within the TS environment.
 */

#include "parquetjni/s3file.h"

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <aws/core/Aws.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "parquetjni/memory_tracking.h"
#include "parquetjni/util.h"

namespace S3Model = Aws::S3::Model;

using Buffer = arrow::Buffer;
using ResizableBuffer = arrow::ResizableBuffer;
template <typename T>
using Result = arrow::Result<T>;
using Status = arrow::Status;

constexpr char kAuthHeader[] = "Authorization";

inline Aws::String ToAwsString(const std::string &s) {
  // Direct construction of Aws::String from std::string doesn't work because
  // it uses a specific Allocator class.
  return Aws::String(s.begin(), s.end());
}

std::string FormatRange(int64_t start, int64_t length) {
  // Format a HTTP range header value
  std::stringstream ss;
  ss << "bytes=" << start << "-" << start + length - 1;
  return ss.str();
}

/// Inject a custom authorization header into an S3 request.
///
/// The AWS C++ SDK doesn't make it easy to attach arbitrary headers
/// to a request within a narrow scope. Overriding the global client's
/// HTTP client factory lets you do this on a global basis
/// only. Modifying the headers on a request object will not work as
/// the value does not persist. Instead, this template generates a
/// subclass of an S3 request that will inject the headers.
template <typename T>
class WithAuth : public T {
 public:
  explicit WithAuth(Aws::String auth_token)
      : T(), auth_token_(std::move(auth_token)) {}

  Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override {
    Aws::Http::HeaderValueCollection headers = T::GetRequestSpecificHeaders();
    headers.emplace(Aws::Http::HeaderValuePair(kAuthHeader, auth_token_));
    return headers;
  }

 private:
  Aws::String auth_token_;
};

arrow::Status GetObjectRange(Aws::S3::S3Client *client,
                             const Aws::String &access_token,
                             const S3Path &path, int64_t start, int64_t length,
                             S3Model::GetObjectResult *out) {
  const auto profile_start = std::chrono::steady_clock::now();
  WithAuth<S3Model::GetObjectRequest> req(access_token);
  req.GetHeaders().emplace(
      Aws::Http::HeaderValuePair(kAuthHeader, access_token));
  req.SetBucket(ToAwsString(path.bucket));
  req.SetKey(ToAwsString(path.key));
  req.SetRange(ToAwsString(FormatRange(start, length)));
  auto result = client->GetObject(req);
  if (!result.IsSuccess()) {
    return arrow::Status::IOError("Could not read file", path.bucket, path.key,
                                  result.GetError());
  }
  *out = result.GetResultWithOwnership();
  TRACE_WITH("S3File::GetObjectRange", path.key, profile_start, load_end);
  return arrow::Status::OK();
}

class ObjectInputFile : public arrow::io::RandomAccessFile {
 public:
  ObjectInputFile(std::shared_ptr<Aws::S3::S3Client> client,
                  const Aws::String access_token, const S3Path &path)
      : client_(std::move(client)),
        access_token_(std::move(access_token)),
        path_(path) {}

  arrow::Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    WithAuth<S3Model::HeadObjectRequest> req(access_token_);
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));

    auto outcome = client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
      if (outcome.GetError().GetErrorType() ==
              Aws::S3::S3Errors::NO_SUCH_BUCKET ||
          outcome.GetError().GetErrorType() ==
              Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        return arrow::Status::IOError("Path does not exist bucket: '",
                                      path_.bucket, "', key: ", path_.key);
      } else {
        return arrow::Status::IOError("When reading information for key '",
                                      path_.key, "' in bucket '", path_.bucket,
                                      "': ", outcome.GetError());
      }
    }
    content_length_ = outcome.GetResult().GetContentLength();
    return arrow::Status::OK();
  }

  arrow::Status CheckClosed() const {
    if (closed_) {
      return arrow::Status::Invalid("Operation on closed stream");
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckPosition(int64_t position, const char *action) const {
    if (position < 0) {
      return arrow::Status::Invalid("Cannot ", action,
                                    " from negative position");
    }
    if (position > content_length_) {
      return arrow::Status::IOError("Cannot ", action, " past end of file");
    }
    return arrow::Status::OK();
  }

  // RandomAccessFile APIs

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    S3Model::GetObjectResult result;
    RETURN_NOT_OK(GetObjectRange(client_.get(), access_token_, path_, position,
                                 nbytes, &result));

    auto &stream = result.GetBody();
    stream.read(reinterpret_cast<char *>(out), nbytes);
    // NOTE: the stream is a stringstream by default, there is no actual error
    // to check for.  However, stream.fail() may return true if EOF is reached.
    return stream.gcount();
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position,
                                         int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> buf,
                          AllocateResizableBuffer(nbytes, GetTrackedPool()));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return buf;
  }

  Result<int64_t> Read(int64_t nbytes, void *out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return buffer;
  }

 protected:
  std::shared_ptr<Aws::S3::S3Client> client_;
  Aws::String access_token_;
  S3Path path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};

arrow::Status OpenS3File(const S3Path &path, const std::string &endpoint,
                         const std::string &access_token,
                         std::shared_ptr<arrow::io::RandomAccessFile> *out) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.endpointOverride = Aws::String(endpoint.begin(), endpoint.end());
  auto s3_client = std::make_shared<Aws::S3::S3Client>(
      Aws::Auth::AWSCredentials(), clientConfig,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
  auto file = new ObjectInputFile(
      std::move(s3_client),
      Aws::String(access_token.begin(), access_token.end()), path);
  ARROW_RETURN_NOT_OK(file->Init());
  *out = std::shared_ptr<ObjectInputFile>(std::move(file));
  return arrow::Status::OK();
}
