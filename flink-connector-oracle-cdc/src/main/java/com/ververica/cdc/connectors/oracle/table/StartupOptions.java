/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.table;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** oracle startup options. */
public class StartupOptions {
    public final StartupMode startupMode;
    public final Long offset;
    public final Long timestamp;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest binlog.
     */
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the beginning of the binlog. This should be used with care, as it is only valid when the
     * binlog is guaranteed to contain the entire history of the database.
     */
    public static StartupOptions earliest() {
        return new StartupOptions(StartupMode.EARLIEST_OFFSET, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the binlog which means only have the changes since the connector was started.
     */
    public static StartupOptions latest() {
        return new StartupOptions(StartupMode.LATEST_OFFSET, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified offset.
     */
    public static StartupOptions specificOffset(long offset) {
        return new StartupOptions(StartupMode.SPECIFIC_OFFSET, offset, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified timestamp.
     *
     * <p>The consumer will traverse the binlog from the beginning and ignore change events whose
     * timestamp is smaller than the specified timestamp.
     *
     * @param timestamp timestamp for the startup offsets, as milliseconds from epoch.
     */
    public static StartupOptions timestamp(long timestamp) {
        return new StartupOptions(StartupMode.TIMESTAMP, null, timestamp);
    }

    private StartupOptions(StartupMode startupMode, Long offset, Long timestamp) {
        this.startupMode = startupMode;
        this.offset = offset;
        this.timestamp = timestamp;

        switch (startupMode) {
            case INITIAL:
            case EARLIEST_OFFSET:
            case LATEST_OFFSET:
                break;
            case SPECIFIC_OFFSET:
                checkNotNull(offset, "offset shouldn't be null");
                break;
            case TIMESTAMP:
                checkNotNull(timestamp, "timestamp(millis) shouldn't be null");
                break;
            default:
                throw new UnsupportedOperationException(startupMode + " mode is not supported.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode
                && Objects.equals(offset, that.offset)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode, offset, timestamp);
    }
}
