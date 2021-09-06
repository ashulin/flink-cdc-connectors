/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of basic task for Oracle connector.
 *
 * <p>Copied from Debezium project. Make it support earliest-offset/specific-offset/timestamp
 * startup mode.
 */
public class OracleConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.jdbcConfig();
        jdbcConnection = new OracleConnection(jdbcConfig, () -> getClass().getClassLoader());
        this.schema =
                new OracleDatabaseSchema(
                        connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);
        this.schema.initializeStorage();

        String adapterString = config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER);
        OracleConnectorConfig.ConnectorAdapter adapter =
                OracleConnectorConfig.ConnectorAdapter.parse(adapterString);

        LOGGER.info("Closing connection before starting schema recovery");

        try {
            jdbcConnection.close();
        } catch (SQLException e) {
            throw new DebeziumException(e);
        }

        OffsetContext previousOffset =
                getPreviousOffset(new OracleOffsetContext.Loader(connectorConfig, adapter));

        validateAndLoadDatabaseHistory(
                connectorConfig, (OracleOffsetContext) previousOffset, schema);

        LOGGER.info("Reconnecting after finishing schema recovery");

        try {
            jdbcConnection.setAutoCommit(false);

        } catch (SQLException e) {
            throw new DebeziumException(e);
        }

        validateSnapshotFeasibility(connectorConfig, (OracleOffsetContext) previousOffset);

        taskContext = new OracleTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(connectorConfig.getMaxQueueSize())
                        .loggingContextSupplier(
                                () -> taskContext.configureLoggingContext(CONTEXT_NAME))
                        .build();

        errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();

        EventDispatcher<TableId> dispatcher =
                new EventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        schema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        final OracleStreamingChangeEventSourceMetrics streamingMetrics =
                new OracleStreamingChangeEventSourceMetrics(
                        taskContext, queue, metadataProvider, connectorConfig);

        ChangeEventSourceCoordinator coordinator =
                new ChangeEventSourceCoordinator(
                        previousOffset,
                        errorHandler,
                        OracleConnector.class,
                        connectorConfig,
                        new OracleChangeEventSourceFactory(
                                connectorConfig,
                                jdbcConnection,
                                errorHandler,
                                dispatcher,
                                clock,
                                schema,
                                jdbcConfig,
                                taskContext,
                                streamingMetrics),
                        new OracleChangeEventSourceMetricsFactory(streamingMetrics),
                        dispatcher,
                        schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords =
                records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }

    private boolean validateAndLoadDatabaseHistory(
            OracleConnectorConfig config, OracleOffsetContext offset, OracleDatabaseSchema schema) {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // We are in schema only recovery mode, use the existing redo log position
                // would like to also verify redo log position exists, but it defaults to 0 which is
                // technically valid
                throw new DebeziumException(
                        "Could not find existing redo log information while attempting schema only recovery snapshot");
            }
            LOGGER.info(
                    "Connector started for the first time, database history recovery will not be executed");
            schema.initializeStorage();
            return false;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // But check to see if the server still has those redo log coordinates ...
                if (!isRedoLogAvailable(config, offset)) {
                    throw new DebeziumException(
                            "The connector is trying to read redo log starting at "
                                    + offset.getSourceInfo()
                                    + ", but this is no longer "
                                    + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                }
                LOGGER.info(
                        "The db-history topic is missing but we are in {} snapshot mode. "
                                + "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                        OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            } else {
                throw new DebeziumException(
                        "The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                                + OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return true;
        }
        schema.recover(offset);
        return false;
    }

    /**
     * Determine whether the redo log position as set on the {@link OracleOffsetContext} is
     * available in the server.
     *
     * @return {@code true} if the server has the redo log coordinates, or {@code false} otherwise
     */
    protected boolean isRedoLogAvailable(OracleConnectorConfig config, OracleOffsetContext offset) {
        LcrPosition lcr = offset.getLcrPosition();
        Scn scn;

        if (lcr != null) {
            scn = lcr.getScn();
        } else {
            scn = offset.getScn();
        }

        if (scn == null || scn.isNull()) {
            return false;
        }

        try {
            Scn oldestScn =
                    LogMinerHelper.getFirstOnlineLogScn(
                            jdbcConnection, config.getLogMiningArchiveLogRetention());
            Scn currentScn = LogMinerHelper.getCurrentScn(jdbcConnection);
            // And compare with the one we're supposed to use.
            if (scn.compareTo(oldestScn) < 0 || scn.compareTo(currentScn) > 0) {
                LOGGER.info(
                        "Connector requires redo log scn '{}', but Oracle only has {}-{}",
                        scn,
                        oldestScn,
                        currentScn);
                return false;
            } else {
                LOGGER.info("Oracle has the redo log scn '{}' required by the connector", scn);
                return true;
            }
        } catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private boolean validateSnapshotFeasibility(
            OracleConnectorConfig config, OracleOffsetContext offset) {
        if (offset != null) {
            if (!offset.isSnapshotRunning()) {
                // But check to see if the server still has those binlog coordinates ...
                if (!isRedoLogAvailable(config, offset)) {
                    throw new DebeziumException(
                            "The connector is trying to read binlog starting at "
                                    + offset.getSourceInfo()
                                    + ", but this is no longer "
                                    + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                }
            }
        }
        return false;
    }
}
