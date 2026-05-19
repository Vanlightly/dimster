package dev.dimster.spi.kafka;

/**
 * Static description of what a particular Kafka client version can do.
 *
 * <p>Returned by {@link KafkaClientFactory#capabilities()} and consulted at startup
 * (and at workload-build time) so dimster can fail fast with a clear message when
 * a workload requires a feature - share groups today, possibly others later - that
 * the bound Kafka client version does not support.
 *
 * <p>The version string is also useful for logging and for tagging Prometheus
 * metrics so dashboards can split runs by client version.
 *
 * @param kafkaClientVersion human-readable version of the kafka-clients jar this
 *                           factory was compiled against, e.g. "3.7.2", "4.2.0".
 * @param shareGroupsSupported true iff this Kafka version provides KafkaShareConsumer,
 *                             AcknowledgeType, and the share-group admin APIs.
 */
public record Capabilities(String kafkaClientVersion, boolean shareGroupsSupported) {
}
