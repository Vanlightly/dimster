package dev.dimster.kafka.impl.v4_1;

import dev.dimster.spi.kafka.RawProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka 4.1 implementation of {@link RawProducer}. Pure delegation - the
 * producer API surface is the most stable part of kafka-clients and nothing
 * here is 4.2-specific.
 */
public class V4_1Producer<K, V> implements RawProducer<K, V> {

    private final KafkaProducer<K, V> producer;

    public V4_1Producer(KafkaProducer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return producer.send(record, callback);
    }

    @Override
    public void initTransactions() {
        producer.initTransactions();
    }

    @Override
    public void beginTransaction() {
        producer.beginTransaction();
    }

    @Override
    public void commitTransaction() {
        producer.commitTransaction();
    }

    @Override
    public void abortTransaction() {
        producer.abortTransaction();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        // Match the original KafkaProducerWrapper.close() timeout.
        producer.close(Duration.ofSeconds(30));
    }
}
