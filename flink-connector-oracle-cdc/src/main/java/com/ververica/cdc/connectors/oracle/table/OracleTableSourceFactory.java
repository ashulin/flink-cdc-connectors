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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.ververica.cdc.debezium.table.DebeziumOptions;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.PORT;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.SCAN_STARTUP_TIMESTAMP;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceOptions.USERNAME;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

/** Factory for creating configured instance of {@link OracleTableSource}. */
public class OracleTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "oracle-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();

        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);

        String schemaName = config.get(SCHEMA_NAME);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);

        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        StartupOptions startupOptions = getStartupOptions(config);

        return new OracleTableSource(
                resolvedSchema,
                hostname,
                port,
                username,
                password,
                databaseName,
                schemaName,
                tableName,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                startupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET);
        options.add(SCAN_STARTUP_TIMESTAMP);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();
            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                String offsetPos = config.get(SCAN_STARTUP_SPECIFIC_OFFSET);
                return StartupOptions.specificOffset(new BigInteger(offsetPos));
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                long millis = config.get(SCAN_STARTUP_TIMESTAMP);
                return StartupOptions.timestamp(millis);
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }
}
