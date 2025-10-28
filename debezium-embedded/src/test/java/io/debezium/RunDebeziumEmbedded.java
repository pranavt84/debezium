/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

public class RunDebeziumEmbedded {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunDebeziumEmbedded.class);

    public static Properties getMySQLProperties() {
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        // Offset storage config
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "10");

        // Database connection config
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "debezium");
        props.setProperty("database.password", "dbz");
        props.setProperty("database.server.id", "85744");
        props.setProperty("topic.prefix", "my-app-connector");

        // Schema history storage
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "/tmp/debezium-logs/schemahistory.dat");

        return props;
    }

    public static Properties getMariaProperties() {

        final Properties props = new Properties();

        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mariadb.MariaDbConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/maria-db/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3309");
        props.setProperty("database.user", "test_user");
        props.setProperty("database.password", "test_user_password");
        props.setProperty("database.server.id", "85745");
        props.setProperty("topic.prefix", "my-app-connector-maria");
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "/tmp/maria-db/schema-history/schemahistory.dat");
        // Optional but recommended:
        props.setProperty("database.server.name", "mariadb_server1");
        props.setProperty("column.propagate.source.type", ".*");
        // props.setProperty("database.include.list", "test_db");
        // props.setProperty("snapshot.mode", "initial");

        return props;
    }

    /*
     * public static Properties getPostgreSQLProperties() {
     * final Properties props = new Properties();
     *
     * props.setProperty("name", "postgresql-engine");
     * props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
     *
     * // Offset storage config
     * props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
     * props.setProperty("offset.storage.file.filename", "/tmp/postgresql/offsets.dat");
     *
     * // Database connection config
     * props.setProperty("database.hostname", "localhost");
     * props.setProperty("database.port", "5432");
     * props.setProperty("database.user", "debezium");
     * props.setProperty("database.password", "debezium");
     * props.setProperty("database.dbname", "postgres");
     * props.setProperty("database.server.name", "postgresql-server");
     *
     * // Replication slot settings
     * props.setProperty("slot.name", "debezium_slot");
     * props.setProperty("plugin.name", "pgoutput");
     * props.setProperty("publication.name", "debezium_pub");
     * // Table filtering
     * props.setProperty("table.include.list", "public.users,public.products");
     *
     * props.setProperty("topic.prefix", "my-app-connector");
     *
     * props.setProperty("table.include.list", ".*");
     *
     * // Snapshot settings
     * props.setProperty("snapshot.mode", "initial");
     * props.setProperty("snapshot.delay.ms", "1000");
     *
     * props.setProperty("include.unknown.datatypes", "true");
     *
     * // Schema history storage
     * props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
     * props.setProperty("schema.history.internal.file.filename", "/tmp/postgresql/schema-history/schemahistory.dat");
     *
     * // Additional settings
     * props.setProperty("tombstones.on.delete", "false");
     * props.setProperty("include.schema.changes", "true");
     * props.setProperty("include.query", "false");
     * props.setProperty("provide.transaction.metadata", "true");
     *
     * // Debug logging
     * props.setProperty("log.include.query", "true");
     * props.setProperty("log.include.schema.changes", "true");
     *
     * return props;
     * }
     */

    public static Properties getPostgreSQLProperties() {
        final Properties props = new Properties();

        // Basic connector setup
        props.setProperty("name", "postgresql-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        // Offset storage
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/postgresql/offsets.dat");

        // Database connection
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5432");
        props.setProperty("database.user", "debezium");
        props.setProperty("database.password", "debezium");
        props.setProperty("database.dbname", "postgres");
        props.setProperty("database.server.name", "postgresql-server");

        // Replication settings
        props.setProperty("slot.name", "debezium_slot");
        props.setProperty("plugin.name", "pgoutput");
        //
        props.setProperty("logical.replication.mode", "in-progress");
        props.setProperty("publication.name", "debezium_pub");

        // Table filtering (choose one)
        props.setProperty("table.include.list", ".*");

        // Topic prefix
        props.setProperty("topic.prefix", "my-app-connector");

        // Snapshot settings
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("snapshot.delay.ms", "1000");

        // Schema history
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "/tmp/postgresql/schema-history/schemahistory.dat");

        // Additional settings
        props.setProperty("tombstones.on.delete", "false");
        props.setProperty("include.schema.changes", "true");
        props.setProperty("include.query", "false");
        props.setProperty("provide.transaction.metadata", "true");

        return props;
    }

    private static final String OUTPUT_FILE = "debezium_output.txt"; // <-- Use .txt file
    private static final int BATCH_SIZE = 1;

    public static void main(String[] args) throws Exception {

        // Choose which database to use
        final Properties props = getPostgreSQLProperties(); // Change this to getMySQLProperties() or getMariaProperties()

        /*
         * try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
         * .using(props)
         * .notifying(record -> {
         * LOGGER.info( " pranav {}", record);
         * })
         * .build()) {
         *
         * // Run the engine asynchronously
         * ExecutorService executor = Executors.newSingleThreadExecutor();
         * executor.execute(engine);
         *
         * // You can add a shutdown hook or blocking logic here if needed
         * }
         */

        List<String> buffer = new ArrayList<>(BATCH_SIZE);
        AtomicInteger totalWritten = new AtomicInteger(0);

        BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE, true)); // append mode

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    synchronized (buffer) {
                        // LOGGER.info( " pranav {}", record.value());
                        buffer.add(record.value());

                        if (buffer.size() >= BATCH_SIZE) {
                            flushBuffer(buffer, writer, totalWritten);
                        }
                    }
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    private static void flushBuffer(List<String> buffer, BufferedWriter writer, AtomicInteger totalWritten) {
        try {
            for (String record : buffer) {
                writer.write(record);
                writer.newLine();
            }
            writer.flush();
            int writtenNow = buffer.size();
            totalWritten.addAndGet(writtenNow);
            System.out.println("Flushed " + writtenNow + " records to file. Total written: " + totalWritten.get());
            buffer.clear();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
