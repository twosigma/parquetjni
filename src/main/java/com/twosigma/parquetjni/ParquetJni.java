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

package com.twosigma.parquetjni;

import java.io.IOException;

final class ParquetJni {
    private ParquetJni() {
        throw new AssertionError("Do not instantiate this class.");
    }

    /**
     * Create a new Parquet reader for an S3 path.
     *
     * @param bucket
     * @param key
     * @param authToken
     * @return The opaque pointer to the native Parquet reader. Must be cleaned up with {@link
     *     #closeJni(long)}.
     * @throws IOException if the reader could not be created.
     */
    static native long allocate(String endpoint, String bucket, String key, String authToken)
            throws IOException;

    /**
     * Create a new Parquet reader for a local file path.
     *
     * @param path
     * @return
     * @throws IOException
     */
    static native long allocate(String path) throws IOException;

    /**
     * Create a new Parquet reader for a list of paths in S3.
     *
     * @param authToken
     * @param bucket
     * @param key
     * @return
     * @throws IOException
     */
    static native long allocate(String endpoint, String authToken, String bucket, String[] key)
            throws IOException;

    /**
     * Get a record batch reader for the given time range.
     *
     * <p>Can filter row groups based on Parquet metadata statistics.
     *
     * @param nativeReaderAddress The opaque pointer to the native Parquet reader.
     * @param columns The columns to load. If null or empty, load all columns.
     * @param timeColumn The time column to filter on, or an empty string to not filter.
     * @param minUtcNanos The minimum value to accept (ignored if timeColumn is empty)
     * @param maxUtcNanos The maximum value to accept (ignored if timeColumn is empty)
     * @param filterRowGroups If true, filter out row groups using metadata. Otherwise use
     *     <tt>ReadTable</tt> which does not permit this, but is faster.
     * @return An opaque pointer to the native record batch reader. MUST be freed with {@link
     *     #closeRecordBatchReader(long)}.
     * @throws IOException if an error occurs.
     */
    static native long getRecordBatchReader(
            long nativeReaderAddress,
            String[] columns,
            String timeColumn,
            long minUtcNanos,
            long maxUtcNanos,
            boolean filterRowGroups)
            throws IOException;

    /**
     * Get the Arrow schema from the record batch reader.
     *
     * @param nativeReaderAddress The opaque pointer to the native record batch reader.
     * @return The serialized Arrow schema.
     * @throws IOException if an error occurs.
     */
    static native byte[] readReaderSchema(long nativeReaderAddress) throws IOException;

    /**
     * Free a record batch reader.
     *
     * @param nativeReaderAddress The opaque pointer to the native record batch reader.
     * @throws IOException if an error occurs.
     */
    static native void closeRecordBatchReader(long nativeReaderAddress) throws IOException;

    /**
     * Get the next Arrow record batch.
     *
     * @param recordBatchReaderAddress The opaque pointer to the native record batch reader.
     * @return The serialized record batch.
     * @throws IOException if an error occurs.
     */
    static native byte[] nextRecordBatch(long recordBatchReaderAddress) throws IOException;

    /**
     * Free the Parquet reader.
     *
     * @param nativeReaderAddress The opaque pointer to the native Parquet reader.
     * @throws IOException if an error occurs.
     */
    static native void closeJni(long nativeReaderAddress) throws IOException;

    /**
     * Get the high water mark of C++-allocated memory so far.
     *
     * @return The allocated bytes peak.
     */
    static native long maxAllocation();

    /**
     * Get the high water mark of C++-allocated memory so far using a separately tracked pool.
     *
     * @return The allocated bytes peak.
     */
    static native long maxTrackedAllocation();

    /**
     * Reset the high water mark for the separately tracked pool.
     *
     * <p>NOTE: ENSURE NO OUTSTANDING PARQUETJNI OBJECTS ARE ALLOCATED BEFORE CALLING.
     *
     * @return The allocated bytes peak.
     */
    static native void resetTrackedAllocation();
}
