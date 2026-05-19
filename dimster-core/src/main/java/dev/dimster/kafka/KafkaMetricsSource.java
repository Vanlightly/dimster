package dev.dimster.kafka;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

/**
 * Interface for Kafka client wrappers that expose internal JMX-style metrics.
 * PrometheusEndpoint uses this to scrape and export a curated subset of Kafka
 * client metrics (producer batching, consumer fetch latency, throttling, etc.)
 * without needing to know the concrete wrapper type.
 */
public interface KafkaMetricsSource {

    /**
     * Returns the Kafka client's internal metrics snapshot.
     * Delegates to KafkaProducer.metrics(), KafkaConsumer.metrics(), or
     * KafkaShareConsumer.metrics().
     */
    Map<MetricName, ? extends Metric> kafkaMetrics();

    /**
     * A human-readable name identifying this client instance, used as a
     * Prometheus label to distinguish metrics from different clients.
     */
    String clientName();
}
