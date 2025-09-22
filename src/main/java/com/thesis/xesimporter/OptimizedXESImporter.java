package com.thesis.xesimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Streamlined XES importer for large files
 */
public class OptimizedXESImporter {
    private static final Logger logger = LoggerFactory.getLogger(OptimizedXESImporter.class);

    public static void main(String[] args) {
        OptimizedXESImporter importer = new OptimizedXESImporter();
        importer.run();
    }

    public void run() {
        logger.info("=== XES to PostgreSQL Importer ===");

        DatabaseConnection dbConnection = new DatabaseConnection();

        try {
            if (!dbConnection.testConnection()) {
                logger.error("Cannot connect to database. Please check your PostgreSQL server.");
                System.out.println("Error: Cannot connect to database. Please check PostgreSQL is running.");
                return;
            }

            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.println("\n" + "=".repeat(50));
                System.out.println("XES File Import");
                System.out.println("=".repeat(50));
                System.out.println("1. Import XES file");
                System.out.println("2. Exit");
                System.out.print("Enter choice (1-2): ");

                String choice = scanner.nextLine().trim();

                switch (choice) {
                    case "1":
                        processXESFile(dbConnection, scanner);
                        break;
                    case "2":
                        logger.info("Exiting application");
                        return;
                    default:
                        System.out.println("Invalid choice. Please enter 1 or 2.");
                }
            }

        } finally {
            dbConnection.disconnect();
        }
    }

    private void processXESFile(DatabaseConnection dbConnection, Scanner scanner) {
        try {
            System.out.print("\nEnter path to XES file: ");
            String filePath = scanner.nextLine().trim();

            if (filePath.startsWith("\"") && filePath.endsWith("\"")) {
                filePath = filePath.substring(1, filePath.length() - 1);
            }

            File xesFile = new File(filePath);
            if (!xesFile.exists()) {
                System.out.println("Error: File not found.");
                return;
            }

            if (!xesFile.getName().toLowerCase().endsWith(".xes")) {
                System.out.print("Warning: File doesn't have .xes extension. Continue? (y/n): ");
                if (!scanner.nextLine().trim().toLowerCase().startsWith("y")) {
                    return;
                }
            }

            long fileSizeMB = xesFile.length() / (1024 * 1024);
            System.out.printf("File: %s (%.1f MB)\n", xesFile.getName(), fileSizeMB / 1.0);

            if (fileSizeMB > 100) {
                System.out.println("Large file detected. Import may take several minutes.");
            }

            System.out.println("WARNING: This will clear all existing data in the database!");
            System.out.print("Continue? (y/n): ");
            if (!scanner.nextLine().trim().toLowerCase().startsWith("y")) {
                return;
            }

            dbConnection.connect();
            dbConnection.clearExistingData();

            BatchXESProcessor processor = new BatchXESProcessor(dbConnection.getConnection());

            logger.info("Starting import of XES file: {}", xesFile.getName());
            System.out.println("Starting import... Progress shown every 1000 traces.\n");

            long startTime = System.currentTimeMillis();

            try {
                StreamingXESParser.parseXESFileStreaming(xesFile, processor);

                long endTime = System.currentTimeMillis();
                double totalSeconds = (endTime - startTime) / 1000.0;

                logger.info("Import completed in {:.2f} seconds", totalSeconds);
                System.out.printf("\nImport completed successfully in %.2f seconds\n", totalSeconds);

                displayImportStatistics(dbConnection);

            } catch (Exception e) {
                logger.error("Error during import", e);
                System.out.println("Error during import: " + e.getMessage());
                dbConnection.rollback();
                return;
            }

            processor.close();
            logger.info("XES import completed successfully");

        } catch (Exception e) {
            logger.error("Error during XES import", e);
            System.out.println("Error during import: " + e.getMessage());
            dbConnection.rollback();
        }
    }

    private void displayImportStatistics(DatabaseConnection dbConnection) {
        try {
            var conn = dbConnection.getConnection();
            var stmt = conn.createStatement();

            System.out.println("\n" + "=".repeat(40));
            System.out.println("IMPORT STATISTICS");
            System.out.println("=".repeat(40));

            String[] tables = {"log", "trace", "event", "attribute", "extension", "classifier"};

            for (String table : tables) {
                var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
                if (rs.next()) {
                    long count = rs.getLong(1);
                    System.out.printf("%-15s: %,d records\n",
                            table.substring(0, 1).toUpperCase() + table.substring(1), count);
                }
                rs.close();
            }

            // Events per trace statistics
            var rs = stmt.executeQuery(
                    "SELECT AVG(event_count) as avg_events, MIN(event_count) as min_events, MAX(event_count) as max_events " +
                            "FROM (SELECT COUNT(*) as event_count FROM trace_has_event GROUP BY trace_id) as trace_stats");
            if (rs.next()) {
                double avgEvents = rs.getDouble("avg_events");
                int minEvents = rs.getInt("min_events");
                int maxEvents = rs.getInt("max_events");
                System.out.printf("\nEvents per trace: avg=%.1f, min=%d, max=%d\n", avgEvents, minEvents, maxEvents);
            }
            rs.close();

            // Most common event attributes
            rs = stmt.executeQuery(
                    "SELECT a.key, COUNT(*) as usage_count " +
                            "FROM event_has_attribute eha " +
                            "JOIN attribute a ON eha.attr_id = a.id " +
                            "GROUP BY a.key " +
                            "ORDER BY usage_count DESC " +
                            "LIMIT 5");

            System.out.println("\nTop event attributes:");
            while (rs.next()) {
                System.out.printf("  - %s: %,d times\n", rs.getString(1), rs.getLong(2));
            }
            rs.close();

            stmt.close();
            System.out.println("=".repeat(40));

        } catch (SQLException e) {
            logger.warn("Could not display import statistics", e);
        }
    }
}