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

import io.netty.buffer.ArrowBuf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/** */
public class ParquetJniReader implements AutoCloseable {
    private static final boolean TRACE = true;
    private final BufferAllocator allocator;
    private final long nativeReaderAddress;
    private boolean closed = false;

    private ParquetJniReader(BufferAllocator allocator, long nativeReaderAddress)
            throws IOException {
        this.allocator = allocator;
        if (nativeReaderAddress == 0) {
            throw new IOException("Native reader is nullptr!");
        }
        this.nativeReaderAddress = nativeReaderAddress;
    }

    static Schema parseSerializedSchema(byte[] serializedSchema) throws IOException {
        final long start = System.nanoTime();

        final ByteArrayInputStream is = new ByteArrayInputStream(serializedSchema);
        final ReadableByteChannel ch = Channels.newChannel(is);
        final MessageMetadataResult metadata = MessageSerializer.readMessage(new ReadChannel(ch));
        final Message message = metadata.getMessage();
        final Schema result = MessageSerializer.deserializeSchema(message);
        final double duration = Duration.ofNanos(System.nanoTime() - start).toMillis() / 1000.0;
        System.err.println("TRACE: ParquetJniReader::ParseSchema " + duration);
        return result;
    }

    public ArrowRecordBatchReader readAll(ReadOptions options) throws IOException {
        final String[] columns;
        if (options.selectedColumns() != null) {
            columns = options.selectedColumns().toArray(new String[0]);
        } else {
            columns = new String[0];
        }
        final long reader =
                ParquetJni.getRecordBatchReader(
                        nativeReaderAddress,
                        columns,
                        options.timeColumn(),
                        options.timeMinUtcNanos(),
                        options.timeMaxUtcNanos(),
                        options.filterRowGroups());
        if (reader == 0) {
            return new NullReader();
        }
        return new JniReader(allocator, reader);
    }

    public static ParquetJniReader open(
            BufferAllocator allocator, String endpoint, String bucket, String key, String authToken)
            throws IOException {
        System.loadLibrary("parquetjni_lib");
        final long addr = ParquetJni.allocate(endpoint, bucket, key, authToken);
        return new ParquetJniReader(allocator, addr);
    }

    public static ParquetJniReader open(BufferAllocator allocator, Path path) throws IOException {
        System.loadLibrary("parquetjni_lib");
        final long addr = ParquetJni.allocate(path.toAbsolutePath().toString());
        return new ParquetJniReader(allocator, addr);
    }

    public static ParquetJniReader open(
            BufferAllocator allocator,
            String endpoint,
            String authToken,
            String bucket,
            String[] keys)
            throws IOException {
        System.loadLibrary("parquetjni_lib");
        final long addr = ParquetJni.allocate(endpoint, authToken, bucket, keys);
        return new ParquetJniReader(allocator, addr);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed.");
        }
        closed = true;
        ParquetJni.closeJni(nativeReaderAddress);
    }

    public interface ArrowRecordBatchReader extends Iterator<ArrowRecordBatch>, AutoCloseable {
        Schema getSchema();

        @Override
        void close() throws IOException;
    }

    static class NullReader implements ArrowRecordBatchReader {

        @Override
        public Schema getSchema() {
            return new Schema(Collections.emptyList());
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ArrowRecordBatch next() {
            throw new IllegalStateException("Nothing to return");
        }
    }

    static class JniReader implements ArrowRecordBatchReader {
        private final BufferAllocator allocator;
        private final Schema schema;
        private long recordBatchReaderAddress;
        private ArrowRecordBatch nextBatch;

        JniReader(BufferAllocator allocator, long address) throws IOException {
            this.allocator = allocator;
            this.recordBatchReaderAddress = address;
            final byte[] serializedSchema = ParquetJni.readReaderSchema(recordBatchReaderAddress);
            this.schema = parseSerializedSchema(serializedSchema);
            this.advance();
        }

        private void advance() throws IOException {
            if (recordBatchReaderAddress == 0) {
                throw new IllegalStateException("Already freed");
            }

            final byte[] serializedBatch;
            try {
                serializedBatch = ParquetJni.nextRecordBatch(recordBatchReaderAddress);
            } catch (IOException e) {
                this.nextBatch = null;
                throw e;
            }
            if (serializedBatch == null) {
                this.nextBatch = null;
                return;
            }

            final long start = System.nanoTime();
            final ByteArrayInputStream is = new ByteArrayInputStream(serializedBatch);
            final ReadableByteChannel ch = Channels.newChannel(is);
            final MessageMetadataResult result = MessageSerializer.readMessage(new ReadChannel(ch));
            if (result == null) {
                throw new IOException("Invalid or incomplete record batch");
            }
            final Message message = result.getMessage();
            if (message.headerType() != MessageHeader.RecordBatch) {
                throw new IOException("Expected record batch, got " + message.headerType());
            }
            if (message.bodyLength() > is.available()) {
                throw new IOException(
                        "Expected message of length at least "
                                + message.bodyLength()
                                + " but got one of length "
                                + is.available());
            }
            if (message.bodyLength() > Integer.MAX_VALUE) {
                throw new IOException("Cannot deserialize record batch larger than 2GB");
            }

            final ArrowBuf buf = allocator.buffer((int) message.bodyLength());
            ch.read(buf.nioBuffer());
            this.nextBatch = MessageSerializer.deserializeRecordBatch(message, buf);

            if (TRACE) {
                // TODO: trace needs to capture all exit point
                final double duration =
                        Duration.ofNanos(System.nanoTime() - start).toMillis() / 1000.0;
                System.err.println("TRACE: RecordBatchReader::DeserializeRecordBatch " + duration);
            }
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public void close() throws IOException {
            if (recordBatchReaderAddress != 0) {
                final long address = recordBatchReaderAddress;
                recordBatchReaderAddress = 0;
                ParquetJni.closeRecordBatchReader(address);
            }
        }

        @Override
        public boolean hasNext() {
            return nextBatch != null;
        }

        @Override
        public ArrowRecordBatch next() {
            final ArrowRecordBatch result = nextBatch;
            if (result == null) {
                throw new IllegalStateException("No more batches");
            }
            try {
                advance();
            } catch (IOException e) {
                throw new RuntimeException("Failure to read next batch", e);
            }
            return result;
        }
    }
}
