package dev.dimster.spi.kafka;

import java.util.Properties;

/**
 * Entry point of the Kafka SPI. One implementation per supported Kafka client
 * version, bound at startup by {@link java.util.ServiceLoader}.
 *
 * <p>Each per-version impl module ({@code dimster-kafka-3.7}, {@code dimster-kafka-4.2.0},
 * ...) ships:
 * <ul>
 *   <li>A class implementing this interface (e.g. {@code KafkaClientFactoryV3_7}).</li>
 *   <li>A {@code META-INF/services/dev.dimster.spi.kafka.KafkaClientFactory} resource
 *       naming that class.</li>
 * </ul>
 *
 * <p>At runtime, {@code Dimster.main} and {@code WorkerHttpServer.main} resolve the
 * factory via {@code ServiceLoader.load(KafkaClientFactory.class).findFirst()}.
 * Each packaged tarball contains exactly one impl module, so exactly one factory
 * is on the classpath. Mismatch (zero or multiple) is a startup-time failure with
 * a clear error message.
 *
 * <h2>Capabilities</h2>
 *
 * <p>{@link #capabilities()} reports what this Kafka version supports - notably
 * whether share groups exist. dimster-core consults this both at startup (logs
 * the version + capability set) and when a share-group workload is requested
 * (fails fast with a clear "build against 4.2+" message rather than crashing
 * later inside an UnsupportedOperationException from the SPI).
 *
 * <h2>Threading and lifecycle</h2>
 *
 * <p>Factories are stateless and safe to share. The objects they create
 * ({@link RawConsumer}, {@link RawProducer}, etc.) wrap one Kafka client each
 * and inherit its threading model.
 */
public interface KafkaClientFactory {

    /**
     * @return what this Kafka client version supports. Same value on every call.
     */
    Capabilities capabilities();

    /**
     * Constructs a classic Kafka consumer.
     *
     * @param properties Kafka consumer configuration (bootstrap.servers, group.id, ...)
     */
    <K, V> RawConsumer<K, V> newConsumer(Properties properties);

    /**
     * Constructs a Kafka share-group consumer (KIP-932). On builds without
     * share-group support ({@code capabilities().shareGroupsSupported() == false})
     * this throws {@link UnsupportedOperationException}; dimster-core checks
     * capabilities first to avoid the throw.
     */
    <K, V> RawShareConsumer<K, V> newShareConsumer(Properties properties);

    /**
     * Constructs a Kafka producer. Pass a transactional.id in {@code properties}
     * to get a transactional producer; otherwise a regular producer is returned.
     * The SPI does not distinguish at the type level - see {@link RawProducer}.
     */
    <K, V> RawProducer<K, V> newProducer(Properties properties);

    /**
     * Constructs a Kafka admin client.
     */
    RawAdmin newAdmin(Properties properties);
}
