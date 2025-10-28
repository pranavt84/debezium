/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.data.Envelope;

/**
 * Unit tests for {@link PgOutputMessageDecoder#shouldMessageBeSkipped} method.
 *
 * This test class validates the message skipping optimization logic that allows
 * the connector to skip already processed messages during restart without
 * performing expensive WAL position lookups.
 *
 * @author Debezium Contributors
 */
public class PgOutputMessageDecoderTest {

    private static final Lsn TEST_LSN_1000 = Lsn.valueOf(1000L);
    private static final Lsn TEST_LSN_2000 = Lsn.valueOf(2000L);
    private static final Lsn TEST_LSN_3000 = Lsn.valueOf(3000L);
    private static final Lsn TEST_LSN_4000 = Lsn.valueOf(4000L);

    @Mock
    private PostgresConnectorConfig config;

    @Mock
    private PostgresSchema schema;

    @Mock
    private PostgresConnection connection;

    @Mock
    private WalPositionLocator walPositionLocator;

    @Mock
    private PostgresOffsetContext offsetContext;

    private PgOutputMessageDecoder decoder;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup default config behavior
        when(config.getSkippedOperations()).thenReturn(EnumSet.noneOf(Envelope.Operation.class));

        // Create decoder with mocked dependencies
        MessageDecoderContext decoderContext = new MessageDecoderContext(config, schema);
        decoder = new PgOutputMessageDecoder(decoderContext, connection);
        decoder.setOffsetContext(offsetContext);
    }

    // ==================== SCENARIO 1: NULL OFFSET CONTEXT ====================

    /**
     * Test Case 1.1: When offset context is null, no messages should be skipped
     * This represents a fresh connector start with no previous state.
     */
    @Test
    public void shouldNotSkipMessagesWhenOffsetContextIsNull() {
        // Given: No offset context (fresh start)
        decoder.setOffsetContext(null);
        ByteBuffer insertMessage = createInsertMessage();

        // When: Checking if message should be skipped
        boolean shouldSkip = decoder.shouldMessageBeSkipped(insertMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    /**
     * Test Case 1.2: Multiple message types with null offset context
     */
    @Test
    public void shouldNotSkipAnyMessageTypesWhenOffsetContextIsNull() {
        // Given: No offset context
        decoder.setOffsetContext(null);

        // When & Then: Various message types should not be skipped
        assertThat(decoder.shouldMessageBeSkipped(createInsertMessage(), TEST_LSN_2000, TEST_LSN_1000, walPositionLocator)).isFalse();
        assertThat(decoder.shouldMessageBeSkipped(createUpdateMessage(), TEST_LSN_2000, TEST_LSN_1000, walPositionLocator)).isFalse();
        assertThat(decoder.shouldMessageBeSkipped(createDeleteMessage(), TEST_LSN_2000, TEST_LSN_1000, walPositionLocator)).isFalse();
        assertThat(decoder.shouldMessageBeSkipped(createBeginMessage(), TEST_LSN_2000, TEST_LSN_1000, walPositionLocator)).isFalse();
        assertThat(decoder.shouldMessageBeSkipped(createCommitMessage(), TEST_LSN_2000, TEST_LSN_1000, walPositionLocator)).isFalse();
    }

    // ==================== SCENARIO 2: NULL LAST PROCESSED LSN ====================

    /**
     * Test Case 2.1: When lastProcessedLsn is null, no messages should be skipped
     * This can happen during initial processing or after certain error conditions.
     */
    @Test
    public void shouldNotSkipMessagesWhenLastProcessedLsnIsNull() {
        // Given: Offset context exists but lastProcessedLsn is null
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(null);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        ByteBuffer insertMessage = createInsertMessage();

        // When: Checking if message should be skipped
        boolean shouldSkip = decoder.shouldMessageBeSkipped(insertMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 3: MESSAGE ALREADY PROCESSED ====================

    /**
     * Test Case 3.1: INSERT message with LSN less than lastProcessedLsn should be skipped
     */
    @Test
    public void shouldSkipInsertMessageWhenLsnIsLessThanLastProcessed() {
        // Given: Last processed LSN is 3000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer insertMessage = createInsertMessage();

        // When: Message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(insertMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();
    }

    /**
     * Test Case 3.2: UPDATE message with LSN less than lastProcessedLsn should be skipped
     */
    @Test
    public void shouldSkipUpdateMessageWhenLsnIsLessThanLastProcessed() {
        // Given: Last processed LSN is 3000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer updateMessage = createUpdateMessage();

        // When: Message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(updateMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();
    }

    /**
     * Test Case 3.3: DELETE message with LSN less than lastProcessedLsn should be skipped
     */
    @Test
    public void shouldSkipDeleteMessageWhenLsnIsLessThanLastProcessed() {
        // Given: Last processed LSN is 3000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer deleteMessage = createDeleteMessage();

        // When: Message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(deleteMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();
    }

    // ==================== SCENARIO 4: MESSAGE NOT YET PROCESSED ====================

    /**
     * Test Case 4.1: INSERT message with LSN greater than lastProcessedLsn should not be skipped
     */
    @Test
    public void shouldNotSkipInsertMessageWhenLsnIsGreaterThanLastProcessed() {
        // Given: Last processed LSN is 2000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer insertMessage = createInsertMessage();

        // When: Message LSN (3000) is greater than last processed LSN (2000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(insertMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    /**
     * Test Case 4.2: UPDATE message with LSN greater than lastProcessedLsn should not be skipped
     */
    @Test
    public void shouldNotSkipUpdateMessageWhenLsnIsGreaterThanLastProcessed() {
        // Given: Last processed LSN is 2000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer updateMessage = createUpdateMessage();

        // When: Message LSN (3000) is greater than last processed LSN (2000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(updateMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    /**
     * Test Case 4.3: DELETE message with LSN greater than lastProcessedLsn should not be skipped
     */
    @Test
    public void shouldNotSkipDeleteMessageWhenLsnIsGreaterThanLastProcessed() {
        // Given: Last processed LSN is 2000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer deleteMessage = createDeleteMessage();

        // When: Message LSN (3000) is greater than last processed LSN (2000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(deleteMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 5: EQUAL LSN EDGE CASE ====================

    /**
     * Test Case 5.1: Message with LSN equal to lastProcessedLsn should be skipped
     * This handles the edge case where LSNs are exactly equal.
     */
    @Test
    public void shouldSkipMessageWhenLsnEqualsLastProcessed() {
        // Given: Last processed LSN is 2000, and we need to set commitLsn first
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer insertMessage = createInsertMessage();

        // When: Message LSN (2000) equals last processed LSN (2000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(insertMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();
    }

    // ==================== SCENARIO 6: TRANSACTION BOUNDARY MESSAGES ====================

    /**
     * Test Case 6.1: BEGIN messages should never be skipped regardless of LSN
     * BEGIN messages are always processed to maintain transaction boundaries.
     */
    @Test
    public void shouldNotSkipBeginMessageEvenWhenLsnIsLessThanLastProcessed() {
        // Given: Last processed LSN is 3000
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        ByteBuffer beginMessage = createBeginMessage();

        // When: BEGIN message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: BEGIN message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    /**
     * Test Case 6.2: COMMIT messages should never be skipped regardless of LSN
     * COMMIT messages are always processed to maintain transaction boundaries.
     */
    @Test
    public void shouldNotSkipCommitMessageEvenWhenLsnIsLessThanLastProcessed() {
        // Given: Last processed LSN is 3000
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        ByteBuffer commitMessage = createCommitMessage();

        // When: COMMIT message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(commitMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: COMMIT message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 7: SPECIAL MESSAGE TYPES ====================

    /**
     * Test Case 7.1: TYPE messages should always be skipped
     */
    @Test
    public void shouldAlwaysSkipTypeMessages() {
        // Given: Any offset context state
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        ByteBuffer typeMessage = createTypeMessage();

        // When: Checking TYPE message
        boolean shouldSkip = decoder.shouldMessageBeSkipped(typeMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // Then: TYPE message should always be skipped
        assertThat(shouldSkip).isTrue();
    }

    /**
     * Test Case 7.2: ORIGIN messages should always be skipped
     */
    @Test
    public void shouldAlwaysSkipOriginMessages() {
        // Given: Any offset context state
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        ByteBuffer originMessage = createOriginMessage();

        // When: Checking ORIGIN message
        boolean shouldSkip = decoder.shouldMessageBeSkipped(originMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // Then: ORIGIN message should always be skipped
        assertThat(shouldSkip).isTrue();
    }

    /**
     * Test Case 7.3: RELATION messages should never be skipped
     */
    @Test
    public void shouldNotSkipRelationMessages() {
        // Given: Last processed LSN is 3000
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        ByteBuffer relationMessage = createRelationMessage();

        // When: RELATION message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(relationMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: RELATION message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 8: TRUNCATE MESSAGES ====================

    /**
     * Test Case 8.1: TRUNCATE messages should be skipped when truncate events are not included
     */
    @Test
    public void shouldSkipTruncateMessagesWhenTruncateEventsNotIncluded() {
        // Given: Truncate events are not included in configuration
        when(config.getSkippedOperations()).thenReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_1000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        ByteBuffer truncateMessage = createTruncateMessage();

        // When: Checking TRUNCATE message
        boolean shouldSkip = decoder.shouldMessageBeSkipped(truncateMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: TRUNCATE message should be skipped
        assertThat(shouldSkip).isTrue();
    }

    /**
     * Test Case 8.2: TRUNCATE messages should follow normal LSN logic when truncate events are included
     */
    @Test
    public void shouldFollowNormalLsnLogicForTruncateMessagesWhenTruncateEventsIncluded() {
        // Given: Truncate events are included and last processed LSN is 3000
        when(config.getSkippedOperations()).thenReturn(EnumSet.noneOf(Envelope.Operation.class));
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer truncateMessage = createTruncateMessage();

        // When: TRUNCATE message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(truncateMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: TRUNCATE message should be skipped due to LSN logic
        assertThat(shouldSkip).isTrue();

        // When: TRUNCATE message LSN (4000) is greater than last processed LSN (3000)
        shouldSkip = decoder.shouldMessageBeSkipped(truncateMessage, TEST_LSN_4000, TEST_LSN_1000, walPositionLocator);

        // Then: TRUNCATE message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 9: LSN SEARCH COMPLETION STATE ====================

    /**
     * Test Case 9.1: Once LSN search is completed, no messages should be skipped
     * This tests the lsnSearchCompleted flag behavior.
     */
    @Test
    public void shouldNotSkipMessagesAfterLsnSearchIsCompleted() {
        // Given: LSN search is completed (simulate by processing one message first)
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_2000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_1000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        // First call completes the LSN search
        ByteBuffer firstMessage = createInsertMessage();
        decoder.shouldMessageBeSkipped(firstMessage, TEST_LSN_3000, TEST_LSN_1000, walPositionLocator);

        // When: Subsequent message with LSN less than last processed
        ByteBuffer secondMessage = createInsertMessage();
        boolean shouldSkip = decoder.shouldMessageBeSkipped(secondMessage, TEST_LSN_1000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped because LSN search is completed
        assertThat(shouldSkip).isFalse();
    }

    // ==================== SCENARIO 10: LOGICAL DECODING MESSAGES ====================

    /**
     * Test Case 10.1: Non-transactional logical decoding messages should follow normal LSN logic
     */
    @Test
    public void shouldFollowNormalLsnLogicForNonTransactionalLogicalDecodingMessages() {
        // Given: Last processed LSN is 3000
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // Process the logical decoding message which sets its own commitLsn for non-transactional
        ByteBuffer logicalDecodingMessage = createLogicalDecodingMessage(false); // non-transactional

        // When: Message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(logicalDecodingMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();

        // When: Message LSN (4000) is greater than last processed LSN (3000)
        shouldSkip = decoder.shouldMessageBeSkipped(logicalDecodingMessage, TEST_LSN_4000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    /**
     * Test Case 10.2: Transactional logical decoding messages should follow normal LSN logic
     */
    @Test
    public void shouldFollowNormalLsnLogicForTransactionalLogicalDecodingMessages() {
        // Given: Last processed LSN is 3000
        when(offsetContext.lastCompletelyProcessedLsn()).thenReturn(TEST_LSN_3000);
        when(offsetContext.lastCommitLsn()).thenReturn(TEST_LSN_2000);

        // First process a BEGIN message to set commitLsn
        ByteBuffer beginMessage = createBeginMessage();
        decoder.shouldMessageBeSkipped(beginMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        ByteBuffer logicalDecodingMessage = createLogicalDecodingMessage(true); // transactional

        // When: Message LSN (2000) is less than last processed LSN (3000)
        boolean shouldSkip = decoder.shouldMessageBeSkipped(logicalDecodingMessage, TEST_LSN_2000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should be skipped
        assertThat(shouldSkip).isTrue();

        // When: Message LSN (4000) is greater than last processed LSN (3000)
        shouldSkip = decoder.shouldMessageBeSkipped(logicalDecodingMessage, TEST_LSN_4000, TEST_LSN_1000, walPositionLocator);

        // Then: Message should not be skipped
        assertThat(shouldSkip).isFalse();
    }

    // ==================== HELPER METHODS FOR MESSAGE CREATION ====================

    private ByteBuffer createInsertMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'I'); // INSERT message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createUpdateMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'U'); // UPDATE message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createDeleteMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'D'); // DELETE message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createBeginMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 'B'); // BEGIN message type
        buffer.putLong(TEST_LSN_2000.asLong()); // commit LSN
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createCommitMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'C'); // COMMIT message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createRelationMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'R'); // RELATION message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createTypeMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'Y'); // TYPE message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createOriginMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'O'); // ORIGIN message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createTruncateMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 'T'); // TRUNCATE message type
        buffer.flip();
        return buffer;
    }

    private ByteBuffer createLogicalDecodingMessage(boolean isTransactional) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put((byte) 'M'); // LOGICAL_DECODING_MESSAGE type
        buffer.put(isTransactional ? (byte) 1 : (byte) 0); // transactional flag
        if (!isTransactional) {
            buffer.putLong(TEST_LSN_2000.asLong()); // commit LSN for non-transactional
        }
        buffer.flip();
        return buffer;
    }
}
