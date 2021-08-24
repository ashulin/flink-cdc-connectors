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

package com.ververica.cdc.connectors.oracle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import com.ververica.cdc.connectors.oracle.debezium.DebeziumUtils;
import com.ververica.cdc.debezium.Validator;
import io.debezium.connector.oracle.OracleConnection;

import java.sql.SQLException;
import java.util.Properties;

/**
 * The validator for Oracle: It also requires the binlog format in the database is ROW and row image
 * is FULL.
 */
public class OracleValidator implements Validator {
    private static final long serialVersionUID = 1L;

    private static final String LOG_FORMAT_ARCHIVE = "ARCHIVELOG";
    private static final String SUPPLEMENTAL_LOG_DATA_ALL_ENABLE = "YES";

    private final Configuration configuration;

    public OracleValidator(Properties properties) {
        this(Configuration.fromMap(Maps.fromProperties(properties)));
    }

    public OracleValidator(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void validate() {
        try (OracleConnection connection = DebeziumUtils.openOracleConnection(configuration)) {
            DatabaseView view = getOracleDatabaseView(connection);
            checkLogMode(view.logMode);
            checkSupplementalLogDataAll(view.supplementalLogDataAll);
        } catch (SQLException ex) {
            throw new TableException(
                    "Unexpected error while connecting to Oracle and validating", ex);
        }
    }

    /** Check whether the log mode is ARCHIVELOG. */
    private static void checkLogMode(String logMode) {
        if (!LOG_FORMAT_ARCHIVE.equalsIgnoreCase(logMode)) {
            throw new ValidationException(
                    String.format(
                            "The Oracle server is configured with LOG_MODE %s rather than %s, which is "
                                    + "required for this connector to work properly. Change the Oracle configuration to use a "
                                    + "LOG_MODE=ARCHIVELOG and restart the connector.",
                            logMode, LOG_FORMAT_ARCHIVE));
        }
    }

    /** Check whether the log mode is ARCHIVELOG. */
    private static void checkSupplementalLogDataAll(String supplementalLogDataAll) {
        if (!SUPPLEMENTAL_LOG_DATA_ALL_ENABLE.equalsIgnoreCase(supplementalLogDataAll)) {
            throw new ValidationException(
                    String.format(
                            "The Oracle server is configured with SUPPLEMENTAL_LOG_DATA_ALL %s rather than %s, which is "
                                    + "required for this connector to work properly. Change the Oracle configuration to use a "
                                    + "SUPPLEMENTAL_LOG_DATA_ALL=YES and restart the connector.",
                            supplementalLogDataAll, LOG_FORMAT_ARCHIVE));
        }
    }

    private DatabaseView getOracleDatabaseView(OracleConnection connection) throws SQLException {
        return connection.queryAndMap(
                "SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE",
                rs -> rs.next() ? new DatabaseView(rs.getString(1), rs.getString(2)) : null);
    }

    static class DatabaseView {
        /** Archive log mode: NOARCHIVELOG/ARCHIVELOG/MANUAL. */
        private final String logMode;
        /**
         * For all tables with a foreign key, indicates whether all other columns belonging to the
         * foreign key are placed into the redo log if any foreign key columns are modified (YES) or
         * not (NO).
         */
        private final String supplementalLogDataAll;

        public DatabaseView(String logMode, String supplementalLogDataAll) {
            this.logMode = logMode;
            this.supplementalLogDataAll = supplementalLogDataAll;
        }
    }
}
