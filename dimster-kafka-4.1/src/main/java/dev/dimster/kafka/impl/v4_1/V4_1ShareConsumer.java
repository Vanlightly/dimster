package dev.dimster.kafka.impl.v4_1;

import dev.dimster.spi.kafka.AckType;
import dev.dimster.spi.kafka.RawShareConsumer;
import dev.dimster.spi.kafka.ShareAckListener;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Kafka 4.1 implementation of {@link RawShareConsumer}.
 *
 * <p>Almost pure delegation. The only mapping work is in {@link #setAckCallback},
 * which adapts {@link ShareAckListener} (which uses {@link TopicPartition}) to
 * Kafka's {@link AcknowledgementCommitCallback} (which uses
 * {@link TopicIdPartition}, a 4.x-only type) by stripping the topic id. Lossy
 * but the dimster wrapper does not read the topic id today; if we ever need it
 * the SPI listener signature can grow without forcing every impl to retrofit.
 */
public class V4_1ShareConsumer<K, V> implements RawShareConsumer<K, V> {

    private final KafkaShareConsumer<K, V> consumer;

    public V4_1ShareConsumer(KafkaShareConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        // KafkaShareConsumer.subscribe takes a Set per the 4.x API; copy if needed.
        consumer.subscribe(topics instanceof Set<String> s ? s : Set.copyOf(topics));
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    @Override
    public void acknowledge(ConsumerRecord<K, V> record, AckType type) {
        consumer.acknowledge(record, toKafka(type));
    }

    @Override
    public void commitSync() {
        consumer.commitSync();
    }

    @Override
    public void commitAsync() {
        consumer.commitAsync();
    }

    @Override
    public void setAckCallback(ShareAckListener listener) {
        // Adapt SPI listener (TopicPartition) <- Kafka callback (TopicIdPartition).
        consumer.setAcknowledgementCommitCallback(new AcknowledgementCommitCallback() {
            @Override
            public void onComplete(Map<TopicIdPartition, Set<Long>> committed, Exception exception) {
                Map<TopicPartition, Set<Long>> flattened = new LinkedHashMap<>(committed.size());
                for (var e : committed.entrySet()) {
                    flattened.put(e.getKey().topicPartition(), e.getValue());
                }
                listener.onComplete(flattened, exception);
            }
        });
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
        // Match KafkaShareConsumerWrapper.close()'s 60-second timeout.
        consumer.close(Duration.ofSeconds(60));
    }

    private static AcknowledgeType toKafka(AckType t) {
        return switch (t) {
            case ACCEPT -> AcknowledgeType.ACCEPT;
            case RELEASE -> AcknowledgeType.RELEASE;
            case REJECT -> AcknowledgeType.REJECT;
        };
    }
}
