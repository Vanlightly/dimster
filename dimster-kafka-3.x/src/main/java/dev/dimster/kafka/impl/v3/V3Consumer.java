package dev.dimster.kafka.impl.v3;

import dev.dimster.spi.kafka.RawConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Kafka 3.x implementation of {@link RawConsumer}. Pure delegation to the
 * underlying {@link KafkaConsumer} - the classic consumer API has been stable
 * since 2.x for every method dimster uses. Kept as a separate class on purpose
 * so any future 3.x-specific quirk can be absorbed here without affecting
 * other versions. The actual kafka-clients 3.x version on the classpath is
 * selected at build time via the &lt;kafka.version&gt; property in
 * dimster-kafka-3.x/pom.xml.
 */
public class V3Consumer<K, V> implements RawConsumer<K, V> {

    private final KafkaConsumer<K, V> consumer;

    public V3Consumer(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        consumer.subscribe(topics, listener);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        consumer.assign(partitions);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        consumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        consumer.commitAsync(offsets, callback);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return consumer.metrics();
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        // 60-second close to match the dimster wrapper; lets in-flight commits drain
        // rather than be cut off and falsely look like commit failures in stats.
        consumer.close(Duration.ofSeconds(60));
    }
}
