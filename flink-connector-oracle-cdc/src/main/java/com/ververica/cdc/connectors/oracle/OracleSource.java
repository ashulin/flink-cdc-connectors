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

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import com.ververica.cdc.connectors.oracle.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.SourceInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.DATABASE_SERVER_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder to build a SourceFunction which can read snapshot and continue to consume log. */
public class OracleSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link OracleSource}. */
    public static class Builder<T> {

        private String hostname;
        private int port = 1521; // default 1521 port
        private String database; // sid
        private String username;
        private String password;
        private String[] schemaList;
        private String[] tableList;
        private Properties dbzProperties;
        private StartupOptions startupOptions = StartupOptions.initial();
        private DebeziumDeserializationSchema<T> deserializer;

        /** Integer port number of the Oracle database server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** The name of the Oracle database from which to stream the changes. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match schema names to be captured; any
         * schema name not included in the include list will be excluded from capturing. By default
         * all non-system schemas will be captured.
         */
        public Builder<T> schemaList(String... schemaList) {
            this.schemaList = schemaList;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be captured; any table not included in the list will be excluded from
         * capturing. Each identifier is of the form schemaName.tableName. By default the connector
         * will monitor every non-system table in each captured database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /** Name of the Oracle database to use when connecting to the Oracle database server. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the Oracle database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /** The Debezium Oracle connector properties. For example, "log.mining.strategy". */
        public Builder<T> debeziumProperties(Properties properties) {
            this.dbzProperties = properties;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link
         * org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the particular Oracle
            // database
            // server/cluster being captured. The logical name should be unique across all other
            // connectors,
            // since it is used as a prefix for all Kafka topic names emanating from this connector.
            // Only alphanumeric characters and underscores should be used.
            props.setProperty("database.server.name", DATABASE_SERVER_NAME);
            props.setProperty("database.dbname", checkNotNull(database));
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));

            if (schemaList != null) {
                props.setProperty("schema.include.list", String.join(",", schemaList));
            }

            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }
            DebeziumOffset specificOffset = null;
            Configuration configuration = null;
            OracleConnection connection = null;
            switch (startupOptions.startupMode) {
                case INITIAL:
                    props.setProperty("snapshot.mode", "initial");
                    break;
                case LATEST_OFFSET:
                    props.setProperty("snapshot.mode", "schema_only");
                    break;
                    // waite debezium
                case SPECIFIC_OFFSET:
                    props.setProperty("snapshot.mode", "schema_only_recovery");
                    specificOffset = getDebeziumOffset(startupOptions.offset.toString());
                    break;
                case EARLIEST_OFFSET:
                    props.setProperty("snapshot.mode", "schema_only_recovery");
                    configuration = Configuration.fromMap(Maps.fromProperties(props));
                    connection = DebeziumUtils.openOracleConnection(configuration);
                    specificOffset =
                            getDebeziumOffset(
                                    DebeziumUtils.getEarliestScn(connection, configuration)
                                            .toString());
                    break;
                case TIMESTAMP:
                    props.setProperty("snapshot.mode", "schema_only_recovery");
                    configuration = Configuration.fromMap(Maps.fromProperties(props));
                    connection = DebeziumUtils.openOracleConnection(configuration);
                    specificOffset =
                            getDebeziumOffset(
                                    DebeziumUtils.getScnByTimestamp(
                                                    connection, startupOptions.timestamp)
                                            .toString());

                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            if (dbzProperties != null) {
                dbzProperties.forEach(props::put);
            }

            return new DebeziumSourceFunction<>(
                    deserializer, props, specificOffset, new OracleValidator(props));
        }
    }

    private static DebeziumOffset getDebeziumOffset(String offset) {
        DebeziumOffset specificOffset = new DebeziumOffset();
        Map<String, String> sourcePartition = new HashMap<>(2);
        // OracleOffsetContext.SERVER_PARTITION_KEY
        sourcePartition.put("server", DATABASE_SERVER_NAME);
        specificOffset.setSourcePartition(sourcePartition);

        Map<String, Object> sourceOffset = new HashMap<>(4);
        sourceOffset.put(SourceInfo.SCN_KEY, offset);
        sourceOffset.put(SourceInfo.COMMIT_SCN_KEY, offset);
        sourceOffset.put(SourceInfo.SNAPSHOT_KEY, Boolean.TRUE.toString());
        specificOffset.setSourceOffset(sourceOffset);
        return specificOffset;
    }
}
