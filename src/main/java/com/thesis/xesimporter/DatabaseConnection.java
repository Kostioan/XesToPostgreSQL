package com.thesis.xesimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Handles database connection and operations for PostgreSQL
 */
public class DatabaseConnection {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnection.class);

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/FinalDB";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "queque123";

    private Connection connection;

    public void connect() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false);
            logger.info("Successfully connected to database: {}", DB_URL);
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC Driver not found", e);
            throw new SQLException("PostgreSQL JDBC Driver not found", e);
        } catch (SQLException e) {
            logger.error("Failed to connect to database: {}", e.getMessage());
            throw e;
        }
    }

    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Database connection closed");
            } catch (SQLException e) {
                logger.error("Error closing database connection", e);
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void commit() throws SQLException {
        if (connection != null) {
            connection.commit();
            logger.debug("Transaction committed");
        }
    }

    public void rollback() {
        if (connection != null) {
            try {
                connection.rollback();
                logger.debug("Transaction rolled back");
            } catch (SQLException e) {
                logger.error("Error rolling back transaction", e);
            }
        }
    }

    public void clearExistingData() throws SQLException {
        logger.info("Dropping and recreating database tables...");

        try (Statement stmt = connection.createStatement()) {
            // Drop all tables - much faster than DELETE
            String[] dropQueries = {
                    "DROP TABLE IF EXISTS event_has_attribute CASCADE",
                    "DROP TABLE IF EXISTS trace_has_event CASCADE",
                    "DROP TABLE IF EXISTS event CASCADE",
                    "DROP TABLE IF EXISTS event_collection CASCADE",
                    "DROP TABLE IF EXISTS trace_has_attribute CASCADE",
                    "DROP TABLE IF EXISTS log_has_trace CASCADE",
                    "DROP TABLE IF EXISTS trace CASCADE",
                    "DROP TABLE IF EXISTS log_has_attribute CASCADE",
                    "DROP TABLE IF EXISTS classifier CASCADE",
                    "DROP TABLE IF EXISTS log CASCADE",
                    "DROP TABLE IF EXISTS attribute CASCADE",
                    "DROP TABLE IF EXISTS extension CASCADE"
            };

            for (String query : dropQueries) {
                stmt.executeUpdate(query);
                logger.debug("Executed: {}", query);
            }

            // Recreate tables
            recreateTables(stmt);

            commit();
            logger.info("Successfully recreated all tables");
        } catch (SQLException e) {
            rollback();
            logger.error("Error recreating tables", e);
            throw e;
        }
    }

    private void recreateTables(Statement stmt) throws SQLException {
        logger.info("Recreating database schema...");

        String[] createQueries = {
                // Create base tables first
                "CREATE TABLE log (id bigint NOT NULL, name varchar(250) NULL, CONSTRAINT log_PK PRIMARY KEY (id))",

                "CREATE TABLE extension (id bigint NOT NULL, name varchar(50) NOT NULL, prefix varchar(50) NOT NULL, " +
                        "uri varchar(250) NOT NULL, CONSTRAINT extension_PK PRIMARY KEY (id))",

                "CREATE TABLE attribute (id bigint NOT NULL, type varchar(50) NOT NULL, \"key\" varchar(50) NOT NULL, " +
                        "ext_id bigint NULL, parent_id bigint NULL, CONSTRAINT attribute_PK PRIMARY KEY (id))",

                "CREATE TABLE trace (id bigint NOT NULL, CONSTRAINT trace_PK PRIMARY KEY (id))",

                "CREATE TABLE event_collection (id bigint NOT NULL, name varchar(50) NULL, " +
                        "CONSTRAINT event_collection_PK PRIMARY KEY (id))",

                "CREATE TABLE event (id bigint NOT NULL, event_coll_id bigint NULL, CONSTRAINT event_PK PRIMARY KEY (id))",

                // Create tables with foreign keys
                "CREATE TABLE classifier (id bigint NOT NULL, name varchar(50) NOT NULL, keys varchar(250) NOT NULL, " +
                        "log_id bigint NOT NULL, CONSTRAINT classifier_PK PRIMARY KEY (id))",

                "CREATE TABLE log_has_attribute (log_id bigint NOT NULL, trace_global boolean NOT NULL, " +
                        "event_global boolean NOT NULL, attr_id bigint NOT NULL, \"value\" varchar(250) NOT NULL, " +
                        "CONSTRAINT log_has_attribute_PK PRIMARY KEY (log_id, trace_global, event_global, attr_id))",

                "CREATE TABLE log_has_trace (sequence bigint NOT NULL, log_id bigint NOT NULL, trace_id bigint NOT NULL, " +
                        "CONSTRAINT log_has_trace_PK PRIMARY KEY (log_id, sequence))",

                "CREATE TABLE trace_has_attribute (trace_id bigint NOT NULL, attr_id bigint NOT NULL, " +
                        "\"value\" varchar(250) NOT NULL, CONSTRAINT trace_has_attribute_PK PRIMARY KEY (trace_id, attr_id))",

                "CREATE TABLE trace_has_event (sequence bigint NOT NULL, trace_id bigint NOT NULL, event_id bigint NOT NULL, " +
                        "CONSTRAINT trace_has_event_PK PRIMARY KEY (trace_id, sequence))",

                "CREATE TABLE event_has_attribute (event_id bigint NOT NULL, attr_id bigint NOT NULL, " +
                        "\"value\" varchar(250) NOT NULL, CONSTRAINT event_has_attribute_PK PRIMARY KEY (event_id, attr_id))"
        };

        for (String query : createQueries) {
            stmt.executeUpdate(query);
            logger.debug("Created table: {}", query.substring(0, Math.min(50, query.length())) + "...");
        }

        // Add foreign key constraints
        String[] foreignKeyQueries = {
                "ALTER TABLE classifier ADD CONSTRAINT classifier_FK_log_id FOREIGN KEY (log_id) REFERENCES log(id)",
                "ALTER TABLE attribute ADD CONSTRAINT attribute_FK_ext_id FOREIGN KEY (ext_id) REFERENCES extension(id)",
                "ALTER TABLE attribute ADD CONSTRAINT attribute_FK_parent_id FOREIGN KEY (parent_id) REFERENCES attribute(id)",
                "ALTER TABLE log_has_attribute ADD CONSTRAINT log_has_attribute_FK_attr_id FOREIGN KEY (attr_id) REFERENCES attribute(id)",
                "ALTER TABLE log_has_attribute ADD CONSTRAINT log_has_attribute_FK_log_id FOREIGN KEY (log_id) REFERENCES log(id)",
                "ALTER TABLE log_has_trace ADD CONSTRAINT log_has_trace_FK_log_id FOREIGN KEY (log_id) REFERENCES log(id)",
                "ALTER TABLE log_has_trace ADD CONSTRAINT log_has_trace_FK_trace_id FOREIGN KEY (trace_id) REFERENCES trace(id)",
                "ALTER TABLE trace_has_attribute ADD CONSTRAINT trace_has_attribute_FK_attr_id FOREIGN KEY (attr_id) REFERENCES attribute(id)",
                "ALTER TABLE trace_has_attribute ADD CONSTRAINT trace_has_attribute_FK_trace_id FOREIGN KEY (trace_id) REFERENCES trace(id)",
                "ALTER TABLE event ADD CONSTRAINT event_FK_event_coll_id FOREIGN KEY (event_coll_id) REFERENCES event_collection(id)",
                "ALTER TABLE trace_has_event ADD CONSTRAINT trace_has_event_FK_event_id FOREIGN KEY (event_id) REFERENCES event(id)",
                "ALTER TABLE trace_has_event ADD CONSTRAINT trace_has_event_FK_trace_id FOREIGN KEY (trace_id) REFERENCES trace(id)",
                "ALTER TABLE event_has_attribute ADD CONSTRAINT event_has_attribute_FK_attr_id FOREIGN KEY (attr_id) REFERENCES attribute(id)",
                "ALTER TABLE event_has_attribute ADD CONSTRAINT event_has_attribute_FK_event_id FOREIGN KEY (event_id) REFERENCES event(id)"
        };

        for (String query : foreignKeyQueries) {
            stmt.executeUpdate(query);
            logger.debug("Added foreign key constraint");
        }

        logger.info("Database schema recreation completed");
    }

    public boolean testConnection() {
        try {
            connect();
            logger.info("Database connection test successful");
            return true;
        } catch (SQLException e) {
            logger.error("Database connection test failed: {}", e.getMessage());
            return false;
        }
    }
}