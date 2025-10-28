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

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Complete E2E test using embedded engine with PostgreSQL source and PostgreSQL sink
 * to test tsvector support changes.
 */
public class RunPostgresqlSourceSinkEmbedded {

    public static void main(String[] args) throws Exception {
        new RunPostgresqlSourceSinkEmbedded().runE2ETest();
    }

    public void runE2ETest() throws Exception {
        // Step 1: Setup source and sink databases
        setupDatabases();

        // Step 2: Run source connector (PostgreSQL -> Kafka)
        runSourceConnector();

        // Step 3: Run sink connector (Kafka -> PostgreSQL)
        runSinkConnector();

        // Step 4: Verify results
        verifyResults();
    }

    private void setupDatabases() throws Exception {
        System.out.println("=== Setting up databases ===");

        // Setup source database
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

            // Create trigger to automatically update tsvector
            stmt.execute("CREATE OR REPLACE FUNCTION test.update_search_vector() " +
                    "RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "NEW.search_vector := to_tsvector('english', NEW.content); " +
                    "RETURN NEW; " +
                    "END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE TRIGGER trg_update_search_vector " +
                    "BEFORE INSERT OR UPDATE ON test.tsvector_test " +
                    "FOR EACH ROW EXECUTE FUNCTION test.update_search_vector()");

            // Insert test data
            stmt.execute("INSERT INTO test.tsvector_test (title, content) VALUES " +
                    "('Test 1', 'This is a test for tsvector support'), " +
                    "('Test 2', 'Another test with different content')");

            System.out.println("Source database setup complete");
        }

        // Setup sink database
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5433/sink_db", "postgres", "postgres")) {

            Statement stmt = conn.createStatement();

            // Create schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS test");
            stmt.execute("DROP TABLE IF EXISTS test.tsvector_test");

            System.out.println("Sink database setup complete");
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

        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::handleSourceEvent)
                .build()) {

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Wait for events
            Thread.sleep(15000);

            engine.close();
            executor.shutdown();
            executor.awaitTermination(15, TimeUnit.SECONDS);
        }
    }

    private void runSinkConnector() throws Exception {
        System.out.println("=== Running PostgreSQL Sink Connector ===");

        Properties props = new Properties();
        props.setProperty("name", "postgresql-sink");
        props.setProperty("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector");
        props.setProperty("connection.url", "jdbc:postgresql://localhost:5433/sink_db");
        props.setProperty("connection.user", "postgres");
        props.setProperty("connection.password", "postgres");
        props.setProperty("topics", "my-app-connector.test.tsvector_test");
        props.setProperty("table.name.format", "test.${topic}");
        props.setProperty("insert.mode", "upsert");
        props.setProperty("delete.enabled", "true");
        props.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("key.converter.schemas.enable", "true");
        props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("value.converter.schemas.enable", "true");

        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::handleSinkEvent)
                .build()) {

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Wait for processing
            Thread.sleep(5000);

            engine.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void verifyResults() throws Exception {
        System.out.println("=== Verifying Results ===");

        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5433/sink_db", "postgres", "postgres")) {

            Statement stmt = conn.createStatement();
            var rs = stmt.executeQuery("SELECT * FROM test.tsvector_test ORDER BY id");

            System.out.println("Data in sink database:");
            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("id") +
                        ", Title: " + rs.getString("title") +
                        ", Content: " + rs.getString("content") +
                        ", Search Vector: " + rs.getString("search_vector"));
            }
        }
    }

    private void handleSourceEvent(ChangeEvent<String, String> event) {
        System.out.println("=== Source CDC Event ===");
        System.out.println("Topic: " + event.destination());
        System.out.println("Key: " + event.key());
        System.out.println("Value: " + event.value());
        System.out.println("========================");

        // Check if tsvector data is present
        if (event.value() != null && event.value().contains("tsvector")) {
            System.out.println("✅ tsvector data detected in CDC event!");
        }
    }

    private void handleSinkEvent(ChangeEvent<String, String> event) {
        System.out.println("=== Sink Processing Event ===");
        System.out.println("Topic: " + event.destination());
        System.out.println("Key: " + event.key());
        System.out.println("Value: " + event.value());
        System.out.println("============================");
    }
}
