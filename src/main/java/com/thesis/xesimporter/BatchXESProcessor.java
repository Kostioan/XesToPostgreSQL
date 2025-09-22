package com.thesis.xesimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.thesis.xesimporter.StreamingXESParser.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * High-performance XES processor with batch SQL operations
 */
public class BatchXESProcessor implements StreamingXESParser.XESHandler {
    private static final Logger logger = LoggerFactory.getLogger(BatchXESProcessor.class);

    // SQL batch sizes for optimal performance
    private static final int SQL_BATCH_SIZE = 1000;

    private final Connection connection;
    private final Map<String, Long> extensionCache = new HashMap<>();
    private final Map<String, Long> attributeCache = new HashMap<>();

    // Current processing state
    private long currentLogId;
    private int traceSequence = 0;
    private long tracesProcessed = 0;
    private long eventsProcessed = 0;
    private long defaultEventCollectionId;

    // Prepared statements
    private PreparedStatement insertLog;
    private PreparedStatement insertClassifier;
    private PreparedStatement insertExtension;
    private PreparedStatement insertAttribute;
    private PreparedStatement insertLogHasAttribute;
    private PreparedStatement insertTrace;
    private PreparedStatement insertLogHasTrace;
    private PreparedStatement insertTraceHasAttribute;
    private PreparedStatement insertEventCollection;
    private PreparedStatement insertEvent;
    private PreparedStatement insertTraceHasEvent;
    private PreparedStatement insertEventHasAttribute;

    public BatchXESProcessor(Connection connection) throws SQLException {
        this.connection = connection;
        prepareStatements();
        createSequences();
        initializeDefaultEventCollection();
    }

    private void prepareStatements() throws SQLException {
        insertLog = connection.prepareStatement(
                "INSERT INTO log (id, name) VALUES (nextval('log_id_seq'), ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertClassifier = connection.prepareStatement(
                "INSERT INTO classifier (id, name, keys, log_id) VALUES (nextval('classifier_id_seq'), ?, ?, ?)");

        insertExtension = connection.prepareStatement(
                "INSERT INTO extension (id, name, prefix, uri) VALUES (nextval('extension_id_seq'), ?, ?, ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertAttribute = connection.prepareStatement(
                "INSERT INTO attribute (id, type, key, ext_id, parent_id) VALUES (nextval('attribute_id_seq'), ?, ?, ?, ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertLogHasAttribute = connection.prepareStatement(
                "INSERT INTO log_has_attribute (log_id, trace_global, event_global, attr_id, value) VALUES (?, ?, ?, ?, ?)");

        insertTrace = connection.prepareStatement(
                "INSERT INTO trace (id) VALUES (nextval('trace_id_seq'))",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertLogHasTrace = connection.prepareStatement(
                "INSERT INTO log_has_trace (sequence, log_id, trace_id) VALUES (?, ?, ?)");

        insertTraceHasAttribute = connection.prepareStatement(
                "INSERT INTO trace_has_attribute (trace_id, attr_id, value) VALUES (?, ?, ?)");

        insertEventCollection = connection.prepareStatement(
                "INSERT INTO event_collection (id, name) VALUES (nextval('event_collection_id_seq'), ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertEvent = connection.prepareStatement(
                "INSERT INTO event (id, event_coll_id) VALUES (nextval('event_id_seq'), ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);

        insertTraceHasEvent = connection.prepareStatement(
                "INSERT INTO trace_has_event (sequence, trace_id, event_id) VALUES (?, ?, ?)");

        insertEventHasAttribute = connection.prepareStatement(
                "INSERT INTO event_has_attribute (event_id, attr_id, value) VALUES (?, ?, ?)");
    }

    private void createSequences() throws SQLException {
        String[] sequences = {
                "CREATE SEQUENCE IF NOT EXISTS log_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS classifier_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS extension_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS attribute_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS trace_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS event_collection_id_seq START 1",
                "CREATE SEQUENCE IF NOT EXISTS event_id_seq START 1"
        };

        try (var stmt = connection.createStatement()) {
            for (String seq : sequences) {
                stmt.executeUpdate(seq);
            }
            logger.info("Database sequences created successfully");
        }
    }

    private void initializeDefaultEventCollection() throws SQLException {
        insertEventCollection.setString(1, "default");
        insertEventCollection.executeUpdate();

        try (ResultSet rs = insertEventCollection.getGeneratedKeys()) {
            if (rs.next()) {
                defaultEventCollectionId = rs.getLong(1);
                logger.debug("Created default event collection with ID: {}", defaultEventCollectionId);
            }
        }
    }

    @Override
    public void onLogStart(String logName) {
        try {
            insertLog.setString(1, logName);
            insertLog.executeUpdate();

            try (ResultSet rs = insertLog.getGeneratedKeys()) {
                if (rs.next()) {
                    currentLogId = rs.getLong(1);
                    logger.info("Created log with ID: {} and name: {}", currentLogId, logName);
                }
            }
        } catch (SQLException e) {
            logger.error("Error creating log entry", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onExtension(String name, String prefix, String uri) {
        try {
            if (!extensionCache.containsKey(prefix)) {
                insertExtension.setString(1, name);
                insertExtension.setString(2, prefix);
                insertExtension.setString(3, uri);
                insertExtension.executeUpdate();

                try (ResultSet rs = insertExtension.getGeneratedKeys()) {
                    if (rs.next()) {
                        long extId = rs.getLong(1);
                        extensionCache.put(prefix, extId);
                        logger.debug("Inserted extension: {} with ID: {}", name, extId);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error processing extension", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onClassifier(String name, String keys) {
        try {
            insertClassifier.setString(1, name);
            insertClassifier.setString(2, keys);
            insertClassifier.setLong(3, currentLogId);
            insertClassifier.executeUpdate();
            logger.debug("Inserted classifier: {}", name);
        } catch (SQLException e) {
            logger.error("Error processing classifier", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onGlobalAttributes(Map<String, String> traceGlobal, Map<String, String> eventGlobal) {
        try {
            processGlobalAttributes(traceGlobal, true, false);
            processGlobalAttributes(eventGlobal, false, true);

            insertLogHasAttribute.executeBatch();
            insertLogHasAttribute.clearBatch();
        } catch (SQLException e) {
            logger.error("Error processing global attributes", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTrace(XESTrace trace) {
        try {
            long traceId = createTrace();
            processTraceAttributes(traceId, trace.getAttributes());
            processTraceEvents(traceId, trace.getEvents());

            tracesProcessed++;
            eventsProcessed += trace.getEvents().size();

            // Execute SQL batches periodically
            if (tracesProcessed % 100 == 0) {
                executeSQLBatches();

                if (tracesProcessed % 1000 == 0) {
                    logger.info("Processed {} traces, {} events", tracesProcessed, eventsProcessed);
                }
            }

        } catch (SQLException e) {
            logger.error("Error processing trace", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onLogEnd() {
        try {
            executeSQLBatches();
            connection.commit();
            logger.info("Processing completed: {} traces, {} events", tracesProcessed, eventsProcessed);
        } catch (SQLException e) {
            logger.error("Error finalizing import", e);
            throw new RuntimeException(e);
        }
    }

    private void processGlobalAttributes(Map<String, String> attributes, boolean isTraceGlobal, boolean isEventGlobal) throws SQLException {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            long attrId = getOrCreateAttribute(entry.getKey(), "string");
            insertLogHasAttribute.setLong(1, currentLogId);
            insertLogHasAttribute.setBoolean(2, isTraceGlobal);
            insertLogHasAttribute.setBoolean(3, isEventGlobal);
            insertLogHasAttribute.setLong(4, attrId);
            insertLogHasAttribute.setString(5, entry.getValue());
            insertLogHasAttribute.addBatch();
        }
    }

    private long createTrace() throws SQLException {
        insertTrace.executeUpdate();

        try (ResultSet rs = insertTrace.getGeneratedKeys()) {
            if (rs.next()) {
                long traceId = rs.getLong(1);

                insertLogHasTrace.setLong(1, traceSequence++);
                insertLogHasTrace.setLong(2, currentLogId);
                insertLogHasTrace.setLong(3, traceId);
                insertLogHasTrace.executeUpdate();

                return traceId;
            }
        }
        throw new SQLException("Failed to get generated trace ID");
    }

    private void processTraceAttributes(long traceId, Map<String, String> attributes) throws SQLException {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            long attrId = getOrCreateAttribute(entry.getKey(), "string");
            insertTraceHasAttribute.setLong(1, traceId);
            insertTraceHasAttribute.setLong(2, attrId);
            insertTraceHasAttribute.setString(3, entry.getValue());
            insertTraceHasAttribute.addBatch();
        }
    }

    private void processTraceEvents(long traceId, java.util.List<XESEvent> events) throws SQLException {
        int eventSequence = 0;

        for (XESEvent event : events) {
            long eventId = createEvent();

            insertTraceHasEvent.setLong(1, eventSequence++);
            insertTraceHasEvent.setLong(2, traceId);
            insertTraceHasEvent.setLong(3, eventId);
            insertTraceHasEvent.addBatch();

            processEventAttributes(eventId, event.getAttributes());
        }
    }

    private long createEvent() throws SQLException {
        insertEvent.setLong(1, defaultEventCollectionId);
        insertEvent.executeUpdate();

        try (ResultSet rs = insertEvent.getGeneratedKeys()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        throw new SQLException("Failed to get generated event ID");
    }

    private void processEventAttributes(long eventId, Map<String, String> attributes) throws SQLException {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            long attrId = getOrCreateAttribute(entry.getKey(), "string");
            insertEventHasAttribute.setLong(1, eventId);
            insertEventHasAttribute.setLong(2, attrId);
            insertEventHasAttribute.setString(3, entry.getValue());
            insertEventHasAttribute.addBatch();
        }
    }

    private long getOrCreateAttribute(String key, String type) throws SQLException {
        String cacheKey = key + ":" + type;

        if (attributeCache.containsKey(cacheKey)) {
            return attributeCache.get(cacheKey);
        }

        insertAttribute.setString(1, type);
        insertAttribute.setString(2, key);
        insertAttribute.setObject(3, null);
        insertAttribute.setObject(4, null);
        insertAttribute.executeUpdate();

        try (ResultSet rs = insertAttribute.getGeneratedKeys()) {
            if (rs.next()) {
                long attrId = rs.getLong(1);
                attributeCache.put(cacheKey, attrId);
                return attrId;
            }
        }
        throw new SQLException("Failed to get generated attribute ID");
    }

    private void executeSQLBatches() throws SQLException {
        insertTraceHasAttribute.executeBatch();
        insertTraceHasAttribute.clearBatch();

        insertTraceHasEvent.executeBatch();
        insertTraceHasEvent.clearBatch();

        insertEventHasAttribute.executeBatch();
        insertEventHasAttribute.clearBatch();
    }

    public void close() throws SQLException {
        PreparedStatement[] statements = {
                insertLog, insertClassifier, insertExtension, insertAttribute,
                insertLogHasAttribute, insertTrace, insertLogHasTrace, insertTraceHasAttribute,
                insertEventCollection, insertEvent, insertTraceHasEvent, insertEventHasAttribute
        };

        for (PreparedStatement stmt : statements) {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}