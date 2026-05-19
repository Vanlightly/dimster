package dev.dimster.kafka.impl.v4_2_0;

import dev.dimster.spi.kafka.RawConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
 * Kafka 4.2.0 implementation of {@link RawConsumer}. Pure delegation to the
 * underlying {@link KafkaConsumer}. Kept as a separate class on purpose so
 * any future 4.2-specific quirk can be absorbed here without affecting other
 * versions.
 */
public class V4_2_0Consumer<K, V> implements RawConsumer<K, V> {

    private final KafkaConsumer<K, V> consumer;

    public V4_2_0Consumer(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
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
        // 60-second close matches the original KafkaConsumerWrapper.close() timeout.
        // Larger than typical because we want in-flight commits to drain rather than
        // get cut off mid-flight, which would falsely look like a commit failure
        // in the benchmark's stats.
        consumer.close(Duration.ofSeconds(60));
    }
}
