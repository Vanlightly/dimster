package dev.dimster.kafka;

import java.time.Instant;

public interface ConsumerCallback {
    void messageReceived(int payloadSize, long publishTimestampMillis,
                         Instant deliveryTime, Instant processingStartTime);

    default void messageReceived(byte[] payload, String topic,
                                 int partition, long publishTimestampMillis,
                                 Instant deliveryTime, Instant processingStartTime) {
        messageReceived(payload.length, publishTimestampMillis, deliveryTime, processingStartTime);
    }

    void error();

    default void pollCompleted(int messageCount) {}
    default void commitSyncCompleted(long latencyMicros) {}
    default void commitAsyncCompleted() {}
    default void processingTimeCompleted(long latencyMicros) {}
    default void batchReceived(String consumerName, int messageCount) {}
}
