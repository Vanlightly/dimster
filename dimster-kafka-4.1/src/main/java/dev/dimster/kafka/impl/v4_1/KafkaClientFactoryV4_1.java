package dev.dimster.kafka.impl.v4_1;

import dev.dimster.spi.kafka.Capabilities;
import dev.dimster.spi.kafka.KafkaClientFactory;
import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.spi.kafka.RawProducer;
import dev.dimster.spi.kafka.RawShareConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.util.Properties;

/**
 * Kafka 4.1 binding of the dimster Kafka SPI.
 *
 * <p>Discovered at startup via {@link java.util.ServiceLoader}; the
 * META-INF/services entry shipped in this module's resources names this class.
 * Each per-version package tarball contains exactly one impl module, so
 * ServiceLoader sees exactly one factory.
 *
 * <p>Capability set: share groups are supported in 4.1.0, so dimster-core's
 * fail-fast on share-group workloads is a no-op for this build.
 *
 * <p>The reported version comes from {@link AppInfoParser#getVersion()} - the
 * same hook Kafka itself uses for its INFO-level startup banner. This means
 * the Capabilities string always reflects the kafka-clients jar actually on
 * the classpath, not a hardcoded value that could go stale.
 */
public class KafkaClientFactoryV4_1 implements KafkaClientFactory {

    private static final Capabilities CAPABILITIES = new Capabilities(AppInfoParser.getVersion(), true);

    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public <K, V> RawConsumer<K, V> newConsumer(Properties properties) {
        return new V4_1Consumer<>(new KafkaConsumer<>(properties));
    }

    @Override
    public <K, V> RawShareConsumer<K, V> newShareConsumer(Properties properties) {
        return new V4_1ShareConsumer<>(new KafkaShareConsumer<>(properties));
    }

    @Override
    public <K, V> RawProducer<K, V> newProducer(Properties properties) {
        return new V4_1Producer<>(new KafkaProducer<>(properties));
    }

    @Override
    public RawAdmin newAdmin(Properties properties) {
        return new V4_1Admin(AdminClient.create(properties));
    }
}
