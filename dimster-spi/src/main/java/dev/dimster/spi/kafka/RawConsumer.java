package dev.dimster.spi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * SPI for the classic (non-share-group) Kafka consumer.
 *
 * <p>This interface exposes only the Kafka API surface that dimster's wrappers
 * actually call - subscribe/assign, poll, commit, metrics, wakeup/close. Per-version
 * impl modules (dimster-kafka-3.x, dimster-kafka-4.1, dimster-kafka-4.2.0) implement
 * this by delegating each method to the underlying {@code KafkaConsumer} from their
 * pinned kafka-clients version.
 *
 * <p>Why this is narrow: every method here is a piece of API that could in principle
 * drift between Kafka versions. Methods that are pure dimster-side business logic
 * (sleep aggregation, commit-linger gating, batch processing) live in the wrapper
 * classes in dimster-core, not in this SPI. Keeping the boundary narrow means
 * per-version impls are mechanical and short, and no business logic gets duplicated
 * across them.
 *
 * <p>Stable Kafka types ({@link ConsumerRecords}, {@link OffsetAndMetadata},
 * {@link OffsetCommitCallback}, {@link TopicPartition}, {@link Metric},
 * {@link MetricName}) are reused directly to avoid noisy mapping. They have been
 * API-stable across the 3.x and 4.x lines for every method dimster uses, and the SPI
 * pins them at the 3.7 baseline (provided scope in dimster-spi/pom.xml) so accidental
 * use of a newer type would fail the SPI's own compile.
 */
public interface RawConsumer<K, V> extends AutoCloseable {

    /** Subscribes to the given topics with dynamic partition assignment. */
    void subscribe(Collection<String> topics);

    /** Statically assigns the given partitions. Mutually exclusive with subscribe(). */
    void assign(Collection<TopicPartition> partitions);

    /** Polls for records up to the given timeout. Returns an empty batch on timeout. */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * Synchronously commits the given offsets, blocking up to {@code timeout}.
     * Throws on commit failure; the wrapper translates into its callback chain.
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout);

    /**
     * Asynchronously commits the given offsets. {@code callback} is invoked when the
     * commit completes (success or failure).
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /** Returns the consumer's Kafka client metrics. */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Interrupts an in-flight poll() by causing it to throw WakeupException.
     * Used by the wrapper's close() to break out of the poll loop promptly.
     */
    void wakeup();

    @Override
    void close();
}
