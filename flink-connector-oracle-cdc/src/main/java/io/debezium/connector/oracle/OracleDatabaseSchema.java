/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The schema of an Oracle database.
 *
 * <p>Copied from Debezium project. Make it support earliest-offset/specific-offset/timestamp
 * startup mode.
 */
public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    private boolean storageInitializationExecuted = false;

    public OracleDatabaseSchema(
            OracleConnectorConfig connectorConfig,
            SchemaNameAdjuster schemaNameAdjuster,
            TopicSelector<TableId> topicSelector,
            OracleConnection connection) {
        super(
                connectorConfig,
                topicSelector,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        new OracleValueConverters(connectorConfig, connection),
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames()),
                connection.getTablenameCaseInsensitivity(connectorConfig),
                connectorConfig.getKeyMapper());
    }

    public Tables getTables() {
        return tables();
    }

    @Override
    protected DdlParser getDdlParser() {
        return new OracleDdlParser();
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        // just a single table per DDL event for Oracle
        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        } else if (schemaChange.getType() == SchemaChangeEventType.ALTER) {
            tableChanges = new TableChanges();
            tableChanges.alter(table);
        } else if (schemaChange.getType() == SchemaChangeEventType.DROP) {
            tableChanges = new TableChanges();
            tableChanges.drop(table);
        }

        record(schemaChange, tableChanges);
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        storageInitializationExecuted = true;
    }

    public boolean isStorageInitializationExecuted() {
        return storageInitializationExecuted;
    }

    /** Return true if the database history entity exists. */
    public boolean historyExists() {
        return databaseHistory.exists();
    }
}
