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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle redo log source from a logical
 * description.
 */
public class OracleTableSource implements ScanTableSource {

    private final ResolvedSchema resolvedSchema;
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final String database;
    private final String schemaName;
    private final String tableName;
    private final ZoneId serverTimeZone;
    private final Properties dbzProperties;
    private final StartupOptions startupOptions;

    public OracleTableSource(
            ResolvedSchema resolvedSchema,
            String hostname,
            int port,
            String username,
            String password,
            String database,
            String schemaName,
            String tableName,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            StartupOptions startupOptions) {
        this.resolvedSchema = resolvedSchema;
        this.hostname = checkNotNull(hostname);
        this.port = port;
        this.database = checkNotNull(database);
        this.schemaName = checkNotNull(schemaName);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(resolvedSchema.toPhysicalRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer =
                new RowDataDebeziumDeserializeSchema(
                        rowType, typeInfo, ((rowData, rowKind) -> {}), serverTimeZone);

        OracleSource.Builder<RowData> builder =
                OracleSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .database(database)
                        .schemaList(schemaName)
                        .tableList(schemaName + "." + tableName)
                        .debeziumProperties(dbzProperties)
                        .startupOptions(startupOptions)
                        .deserializer(deserializer);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new OracleTableSource(
                resolvedSchema,
                hostname,
                port,
                username,
                password,
                database,
                schemaName,
                tableName,
                serverTimeZone,
                dbzProperties,
                startupOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OracleTableSource)) {
            return false;
        }
        OracleTableSource that = (OracleTableSource) o;
        return port == that.port
                && Objects.equals(resolvedSchema, that.resolvedSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(database, that.database)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(startupOptions, that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resolvedSchema,
                hostname,
                port,
                username,
                password,
                database,
                schemaName,
                tableName,
                serverTimeZone,
                dbzProperties,
                startupOptions);
    }

    @Override
    public String asSummaryString() {
        return "Oracle-CDC";
    }
}
