package dev.dimster.kafka.impl.v3;

import dev.dimster.spi.kafka.Capabilities;
import dev.dimster.spi.kafka.KafkaClientFactory;
import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.spi.kafka.RawProducer;
import dev.dimster.spi.kafka.RawShareConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.util.Properties;

/**
 * Kafka 3.x binding of the dimster Kafka SPI. Single module covering the entire
 * 3.x line (3.0 through 3.9); the concrete kafka-clients version on the classpath
 * is selected at build time via the &lt;kafka.version&gt; property in this module's
 * pom.xml (default: 3.9.0).
 *
 * <p>Discovered at startup via {@link java.util.ServiceLoader}; the
 * META-INF/services entry shipped in this module's resources names this class.
 *
 * <p>The reported version comes from {@link AppInfoParser#getVersion()}, which
 * Kafka itself uses for its INFO-level startup banner. So a build against
 * 3.7.2 reports "3.7.2", a build against 3.9.0 reports "3.9.0", and there's
 * no risk of a stale hardcoded string in capabilities.
 *
 * <p>Capability set: share groups are NOT supported anywhere in 3.x (KIP-932
 * ships in 4.2), so {@link #newShareConsumer} throws
 * {@link UnsupportedOperationException}. dimster-core's capability check at
 * workload-build time should catch share-group workloads before this throw fires;
 * the throw is a backstop with the same remediation message ("build against 4.2+").
 */
public class KafkaClientFactoryV3 implements KafkaClientFactory {

    // Version is read at class-init from the kafka-clients jar's manifest.
    // Computed once and cached.
    private static final Capabilities CAPABILITIES = new Capabilities(AppInfoParser.getVersion(), false);

    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public <K, V> RawConsumer<K, V> newConsumer(Properties properties) {
        return new V3Consumer<>(new KafkaConsumer<>(properties));
    }

    @Override
    public <K, V> RawShareConsumer<K, V> newShareConsumer(Properties properties) {
        throw new UnsupportedOperationException(
                "Share group consumer is not available in the Kafka 3.x client line (KIP-932 ships in 4.2). "
                        + "Build dimster against 4.2+ to run share-group workloads.");
    }

    @Override
    public <K, V> RawProducer<K, V> newProducer(Properties properties) {
        return new V3Producer<>(new KafkaProducer<>(properties));
    }

    @Override
    public RawAdmin newAdmin(Properties properties) {
        return new V3Admin(AdminClient.create(properties));
    }
}
