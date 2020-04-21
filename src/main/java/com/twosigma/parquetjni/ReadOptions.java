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

import com.google.auto.value.AutoValue;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/** Options for reading a Parquet file. */
@AutoValue
public abstract class ReadOptions {
    /** The name of the time column, used for filtering. */
    public abstract @Nullable String timeColumn();

    /** A list of columns to read. If empty or not given, read all columns. */
    public abstract @Nullable List<String> selectedColumns();

    /**
     * The minimum time we care about (in nanoseconds since UTC epoch).
     *
     * <p>If provided with the time column, used to filter row groups.
     */
    public abstract long timeMinUtcNanos();

    /**
     * The minimum time we care about (in nanoseconds since UTC epoch).
     *
     * <p>If provided with the time column, used to filter row groups.
     */
    public abstract long timeMaxUtcNanos();

    /**
     * Whether to use Parquet metadata to filter out row groups.
     *
     * <p>If not, the reader will use a native method which is likely faster than explicitly
     * specifying that all row groups are to be read.
     */
    public abstract boolean filterRowGroups();

    /** Create a new builder. */
    public static Builder newBuilder() {
        return new AutoValue_ReadOptions.Builder()
                .setTimeMinUtcNanos(0)
                .setTimeMaxUtcNanos(0)
                .setFilterRowGroups(false);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setTimeColumn(String timeColumn);

        public abstract Builder setSelectedColumns(List<String> columns);

        public abstract Builder setTimeMinUtcNanos(long time);

        public abstract Builder setTimeMaxUtcNanos(long time);

        public abstract Builder setFilterRowGroups(boolean filterRowGroups);

        public abstract ReadOptions build();
    }
}
