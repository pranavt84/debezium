/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Simple test to verify tsvector support in PostgreSQL source connector
 */
public class RunPostgresqlSourceOnlyEmbedded {

    private final AtomicInteger eventCount = new AtomicInteger(0);
    private final AtomicInteger tsvectorEventCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        new RunPostgresqlSourceOnlyEmbedded().runTest();
    }

    public void runTest() throws Exception {
        // Setup database
        setupDatabase();

        // Run source connector
        runSourceConnector();

        // Print summary
        printSummary();
    }

    private void setupDatabase() throws Exception {
        System.out.println("=== Setting up database ===");

        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/source_db", "postgres", "postgres")) {

            Statement stmt = conn.createStatement();

            // Create schema and table with tsvector
            stmt.execute("CREATE SCHEMA IF NOT EXISTS test");
            stmt.execute("DROP TABLE IF EXISTS test.tsvector_test");
            stmt.execute("CREATE TABLE test.tsvector_test (" +
                    "id SERIAL PRIMARY KEY, " +
                    "title TEXT, " +
                    "content TEXT, " +
                    "search_vector tsvector" +
                    ")");

            // Insert test data
            stmt.execute("INSERT INTO test.tsvector_test (title, content, search_vector) VALUES " +
                    "('Test 1', 'This is a test for tsvector support', to_tsvector('english', 'This is a test for tsvector support')), " +
                    "('Test 2', 'Another test with different content', to_tsvector('english', 'Another test with different content'))");

            System.out.println("✅ Database setup complete - 2 records inserted");
        }
    }

    private void runSourceConnector() throws Exception {
        System.out.println("=== Running PostgreSQL Source Connector ===");

        Properties props = new Properties();
        props.setProperty("name", "postgresql-source");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/postgresql/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");

        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5432");
        props.setProperty("database.user", "postgres");
        props.setProperty("database.password", "postgres");
        props.setProperty("database.dbname", "source_db");
        props.setProperty("database.server.name", "postgres-source");
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("slot.name", "debezium_slot");
        props.setProperty("publication.name", "debezium_pub");

        props.setProperty("schema.include.list", "test");
        props.setProperty("table.include.list", "test.tsvector_test");
        props.setProperty("include.unknown.datatypes", "true");

        props.setProperty("topic.prefix", "my-app-connector");

        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "/tmp/postgresql/schema-history.dat");

        props.setProperty("snapshot.mode", "initial");
        props.setProperty("snapshot.delay.ms", "1000");

        System.out.println("Starting Debezium engine...");

        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::handleEvent)
                .build()) {

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            System.out.println("Waiting for events (20 seconds)...");
            Thread.sleep(20000);

            System.out.println("Shutting down engine...");
            engine.close();
            executor.shutdown();

            boolean terminated = executor.awaitTermination(15, TimeUnit.SECONDS);
            if (!terminated) {
                System.out.println("⚠️  Executor did not terminate gracefully");
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            System.out.println("⚠️  Engine interrupted - this is expected during shutdown");
        }
    }

    private void handleEvent(ChangeEvent<String, String> event) {
        int count = eventCount.incrementAndGet();
        System.out.println("=== CDC Event #" + count + " ===");
        System.out.println("Topic: " + event.destination());
        System.out.println("Key: " + event.key());
        System.out.println("Value: " + event.value());
        System.out.println("=================");

        // Check if tsvector data is present
        if (event.value() != null && event.value().contains("tsvector")) {
            tsvectorEventCount.incrementAndGet();
            System.out.println("✅ tsvector data detected in CDC event!");
        }

        // Check for the logical type
        if (event.value() != null && event.value().contains("io.debezium.data.Tsvector")) {
            System.out.println("✅ tsvector logical type detected!");
        }

        // Check for the actual tsvector content
        if (event.value() != null && event.value().contains("search_vector")) {
            System.out.println("✅ search_vector column detected!");
        }
    }

    private void printSummary() {
        System.out.println("\n=== SUMMARY ===");
        System.out.println("Total events received: " + eventCount.get());
        System.out.println("Events with tsvector: " + tsvectorEventCount.get());

        if (eventCount.get() > 0) {
            System.out.println("✅ CDC events are being generated");
        }
        else {
            System.out.println("❌ No CDC events received");
        }

        if (tsvectorEventCount.get() > 0) {
            System.out.println("✅ tsvector support is working!");
        }
        else {
            System.out.println("❌ No tsvector data detected");
        }
    }
}
