package dev.dimster.spi.kafka;

/**
 * SPI abstraction over Kafka's {@code GroupType} enum, narrowed to the two cases
 * dimster's admin layer actually distinguishes.
 *
 * <p>Why this exists: {@code GroupType.SHARE} is 4.2+, so the SPI cannot reference
 * Kafka's enum directly. Splitting consumer-vs-share at this level also reflects
 * how dimster's KafkaCluster uses the result: it deletes consumer/classic groups and
 * share groups via different admin APIs anyway, so a single boolean-style split is
 * exactly the right granularity.
 */
public enum GroupKind {
    /** Classic and modern (KIP-848) consumer groups - both deleted via deleteConsumerGroups(). */
    CLASSIC_OR_CONSUMER,
    /** Share groups - 4.2+ only; deleted via deleteShareGroups(). */
    SHARE
}
