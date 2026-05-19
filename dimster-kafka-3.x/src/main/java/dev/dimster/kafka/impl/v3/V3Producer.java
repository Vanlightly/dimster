package dev.dimster.kafka.impl.v3;

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
 * Kafka 3.x implementation of {@link RawProducer}. Producer API has been
 * stable since 0.11; this is pure delegation. The concrete 3.x kafka-clients
 * version is selected at build time via &lt;kafka.version&gt; in this module's pom.
 */
public class V3Producer<K, V> implements RawProducer<K, V> {

    private final KafkaProducer<K, V> producer;

    public V3Producer(KafkaProducer<K, V> producer) {
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
        producer.close(Duration.ofSeconds(30));
    }
}
