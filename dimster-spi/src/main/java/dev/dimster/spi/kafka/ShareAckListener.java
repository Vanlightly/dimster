package dev.dimster.spi.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * SPI callback for share-consumer acknowledgement-commit completion.
 *
 * <p>Why this exists: Kafka's {@code AcknowledgementCommitCallback} (4.2+) hands
 * back a {@code Map<TopicIdPartition, Set<Long>>}, but {@code TopicIdPartition} is
 * also 4.2-only, so the SPI cannot use that signature without breaking compilation
 * against 3.7. This listener uses {@link TopicPartition} (stable across all
 * supported Kafka versions); the 4.2 impl flattens the topic-id away when adapting
 * Kafka's callback. Lossy in principle, but dimster's wrappers don't read the
 * topic ID today, so nothing observable is lost.
 *
 * <p>The 3.7 impl never invokes this listener - share consumers don't exist there.
 */
@FunctionalInterface
public interface ShareAckListener {
    /**
     * Called when an async ack-commit either completes or fails.
     *
     * @param committedOffsets per-partition set of acknowledged offsets, or empty on failure
     * @param error            null on success, otherwise the exception from the commit
     */
    void onComplete(Map<TopicPartition, Set<Long>> committedOffsets, Throwable error);
}
