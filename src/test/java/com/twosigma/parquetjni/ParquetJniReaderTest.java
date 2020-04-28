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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** A simple integration test. Expects to run against a properly configured Minio. */
class ParquetJniReaderTest {

    private static final String TOKEN = "";
    private static final String BUCKET = "bucket";
    private static final String[] KEYS = {
        "file_0.parquet",
        "file_1.parquet",
        "file_2.parquet",
        "file_3.parquet",
    };

    @Test
    public void testRead() throws Exception {
        try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            try (final ParquetJniReader file =
                    ParquetJniReader.open(allocator, "http://localhost:9000", TOKEN, BUCKET, KEYS)) {
                final ReadOptions readOptions = ReadOptions.newBuilder().build();
                try (final ParquetJniReader.ArrowRecordBatchReader reader = file.readAll(readOptions)) {
                    final Schema schema = reader.getSchema();
                    Assertions.assertEquals(100, schema.getFields().size());
                    for (final Field field : schema.getFields()) {
                        Assertions.assertTrue(field.getType() instanceof ArrowType.FloatingPoint);
                    }
                    long totalRows = 0;
                    while (reader.hasNext()) {
                        try (final ArrowRecordBatch batch = reader.next()) {
                            totalRows += batch.getLength();
                        }
                    }
                    Assertions.assertEquals(524288, totalRows);
                }
            }
        }
    }
}
