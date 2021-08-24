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

package com.ververica.cdc.connectors.oracle.debezium;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.relational.history.AbstractDatabaseHistory;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Utilities related to Debezium. */
public class DebeziumUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumUtils.class);
    public static final String LOG_VIEW = "V$LOG";
    public static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";

    public static final String LOG_OFFSET_BY_TIME_SQL =
            String.format(
                    "SELECT MIN(FIRST_CHANGE#) FROM ("
                            + "SELECT FIRST_CHANGE# FROM %s WHERE TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') BETWEEN FIRST_TIME AND NVL(NEXT_TIME, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')) "
                            + "UNION SELECT FIRST_CHANGE# FROM %s WHERE TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') BETWEEN FIRST_TIME AND NEXT_TIME)",
                    LOG_VIEW, ARCHIVED_LOG_VIEW);

    /** Creates and opens a new {@link OracleConnection}. */
    public static OracleConnection openOracleConnection(Configuration configuration) {
        OracleConnection jdbc =
                new OracleConnection(
                        oracleConnectorConfig(configuration).jdbcConfig(),
                        DebeziumUtils.class::getClassLoader);
        try {
            jdbc.connect();
        } catch (SQLException e) {
            LOG.error("Failed to open Oracle connection", e);
            throw new FlinkRuntimeException("Failed to open Oracle connection", e);
        }

        return jdbc;
    }

    public static OracleConnectorConfig oracleConnectorConfig(Configuration configuration) {
        return new OracleConnectorConfig(toDebeziumConfig(configuration));
    }

    public static io.debezium.config.Configuration toDebeziumConfig(Configuration configuration) {
        return io.debezium.config.Configuration.from(configuration.toMap())
                .edit()
                .with(AbstractDatabaseHistory.INTERNAL_PREFER_DDL, true)
                .build();
    }

    /**
     * This method fetches the oldest SCN from online redo log files.
     *
     * @param configuration debezium oracle configuration
     * @return oldest SCN from online redo log
     */
    public static Scn getEarliestScn(OracleConnection connection, Configuration configuration) {
        try {
            return LogMinerHelper.getFirstOnlineLogScn(
                    connection,
                    oracleConnectorConfig(configuration).getLogMiningArchiveLogRetention());
        } catch (SQLException e) {
            LOG.error("Failed to get Oracle earliest Scn", e);
            throw new FlinkRuntimeException("Failed to get the earliest Scn", e);
        }
    }

    public static Scn getScnByTimestamp(OracleConnection connection, long timestamp) {
        String time = DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss");
        try (PreparedStatement st =
                connection.connection(false).prepareStatement(LOG_OFFSET_BY_TIME_SQL)) {
            st.setString(1, time);
            st.setString(2, time);
            st.setString(3, time);
            try (ResultSet result = st.executeQuery()) {
                if (!result.next()) {
                    throw new FlinkRuntimeException("Failed to get the Scn by timestamp");
                }
                return Scn.valueOf(result.getLong(1));
            }
        } catch (SQLException e) {
            LOG.error("Failed to get the Scn by timestamp", e);
            throw new FlinkRuntimeException("Failed to get the Scn by timestamp", e);
        }
    }
}
