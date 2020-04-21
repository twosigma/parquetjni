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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
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

        int messageLength = MessageSerializer.bytesToInt(serializedSchema);
        if (messageLength + 4 != serializedSchema.length) {
            throw new IOException(
                    "Expected message of length "
                            + messageLength
                            + " but got one of length "
                            + (serializedSchema.length - 4));
        }
        ByteBuffer bb = ByteBuffer.allocate(messageLength);
        bb.put(serializedSchema, 4, messageLength);
        bb.rewind();
        final Schema result = MessageSerializer.deserializeSchema(Message.getRootAsMessage(bb));
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
            int messageLength = MessageSerializer.bytesToInt(serializedBatch);
            if (messageLength + 4 > serializedBatch.length) {
                throw new IOException(
                        "Expected message of length at least "
                                + messageLength
                                + " but got one of length "
                                + (serializedBatch.length - 4));
            }
            ByteBuffer bb = ByteBuffer.allocate(messageLength);
            bb.put(serializedBatch, 4, messageLength);
            bb.rewind();
            final Message message = Message.getRootAsMessage(bb);
            if (message.headerType() != MessageHeader.RecordBatch) {
                throw new IOException("Expected record batch, got " + message.headerType());
            }
            final int remainingLength = serializedBatch.length - (messageLength + 4);
            if (message.bodyLength() > remainingLength) {
                throw new IOException(
                        "Expected message of length at least "
                                + message.bodyLength()
                                + " but got one of length "
                                + remainingLength);
            }
            if (message.bodyLength() > Integer.MAX_VALUE) {
                throw new IOException("Cannot deserialize record batch larger than 2GB");
            }

            ArrowBuf buf = allocator.buffer((int) message.bodyLength());
            buf.writeBytes(serializedBatch, messageLength + 4, (int) message.bodyLength());
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
