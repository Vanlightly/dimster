package dev.dimster.spi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * SPI for a Kafka share-group consumer (KIP-932, Kafka 4.1+).
 *
 * <p>This interface exists in the SPI so dimster-core can call share-group methods
 * without referencing 4.x-only types ({@code KafkaShareConsumer},
 * {@code AcknowledgeType}, {@code AcknowledgementCommitCallback},
 * {@code TopicIdPartition}). Pre-4.1 impl modules ({@code dimster-kafka-3.x})
 * still need to satisfy {@link KafkaClientFactory#newShareConsumer}, but they throw
 * {@link UnsupportedOperationException} from {@code newShareConsumer} so this
 * interface is never actually instantiated against an older Kafka client. The
 * dimster-core capability check ({@link Capabilities#shareGroupsSupported}) is the
 * primary fail-fast.
 *
 * <p>{@link AckType} mirrors Kafka's {@code AcknowledgeType} (3 values) - the
 * per-version impls map SPI -> Kafka inside acknowledge(). Stable Kafka types
 * ({@link ConsumerRecord}, {@link ConsumerRecords}) are reused directly to avoid
 * mapping overhead in the poll-and-acknowledge hot path.
 */
public interface RawShareConsumer<K, V> extends AutoCloseable {

    /** Subscribes to the given topics. Share-group consumers do not support manual assign(). */
    void subscribe(Collection<String> topics);

    /** Polls for records up to the given timeout. Returns an empty batch on timeout. */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * Acknowledges (or rejects/releases) a single record. {@code record} must come
     * from a prior {@link #poll} on this same consumer instance - the impl passes it
     * straight through to {@code consumer.acknowledge(...)}.
     */
    void acknowledge(ConsumerRecord<K, V> record, AckType type);

    /** Synchronously commits all pending acknowledgements. */
    void commitSync();

    /** Asynchronously commits all pending acknowledgements. Completion is signaled
     *  via the listener registered with {@link #setAckCallback}. */
    void commitAsync();

    /**
     * Registers a listener invoked when an async ack-commit completes. May be
     * called once before the first poll(); the impl forwards it to the underlying
     * consumer's setAcknowledgementCommitCallback().
     */
    void setAckCallback(ShareAckListener listener);

    /** Returns the consumer's Kafka client metrics. */
    Map<MetricName, ? extends Metric> metrics();

    /** Interrupts an in-flight poll() so the wrapper can shut down promptly. */
    void wakeup();

    @Override
    void close();
}
