package dev.dimster.spi.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * SPI for a Kafka producer.
 *
 * <p>{@link ProducerRecord}, {@link RecordMetadata}, and {@link Callback} are stable
 * types reused directly from kafka-clients (provided-scope at the 3.7 baseline).
 * Transactional methods are exposed unconditionally - non-transactional producers
 * simply never have them called (the wrapper guards on its own
 * {@code transactional} flag).
 *
 * <p>Why no separate {@code RawTransactionalProducer}: Kafka's own KafkaProducer
 * exposes both APIs on one type, and the wrapper already discriminates by config.
 * Splitting at the SPI would force two factory methods plus a downcast at the
 * wrapper boundary, with no benefit to either side.
 */
public interface RawProducer<K, V> extends AutoCloseable {

    /**
     * Asynchronously sends a record. {@code callback} is invoked with the broker's
     * acknowledgement metadata on success or the exception on failure.
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * Initialises this producer's transactional state with the broker. Must be
     * called once before the first {@link #beginTransaction()}.
     */
    void initTransactions();

    /** Begins a new transaction. */
    void beginTransaction();

    /** Commits the current transaction. */
    void commitTransaction();

    /** Aborts the current transaction. May fail; callers should log+continue. */
    void abortTransaction();

    /** Returns the producer's Kafka client metrics. */
    Map<MetricName, ? extends Metric> metrics();

    @Override
    void close();
}
