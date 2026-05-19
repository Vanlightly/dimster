package dev.dimster.spi.kafka;

/**
 * SPI mirror of Kafka's {@code org.apache.kafka.clients.consumer.AcknowledgeType}.
 *
 * <p>Why this exists in the SPI: {@code AcknowledgeType} only exists in Kafka 4.2+
 * (it ships alongside share groups). The SPI cannot reference it directly without
 * breaking the 3.7 build of {@code dimster-spi} itself. Per-version impls map this
 * SPI enum to / from the real Kafka enum - the 3.7 impl never gets called with an
 * AckType because share-group consumer creation throws first.
 */
public enum AckType {
    /** Mark the record as successfully processed - it will not be redelivered. */
    ACCEPT,
    /** Release the record back to the share group - it can be redelivered. */
    RELEASE,
    /** Reject the record - it will be sent to the dead-letter queue (if configured). */
    REJECT
}
