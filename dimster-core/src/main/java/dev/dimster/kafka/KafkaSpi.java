package dev.dimster.kafka;

import dev.dimster.spi.kafka.Capabilities;
import dev.dimster.spi.kafka.KafkaClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Bootstrap entry point that resolves the dimster Kafka SPI factory at runtime
 * via {@link ServiceLoader}.
 *
 * <p>Each per-version impl module (dimster-kafka-3.x, dimster-kafka-4.2.0, ...)
 * ships a {@code META-INF/services/dev.dimster.spi.kafka.KafkaClientFactory}
 * resource naming its factory class. Each per-version package tarball bundles
 * exactly one impl module, so at runtime exactly one factory is on the classpath.
 * This class is the single place that does the lookup; everything else in
 * dimster-core calls {@link #factory()} or {@link #capabilities()} from here.
 *
 * <p>Why a static singleton rather than constructor injection: the SPI binding
 * is genuinely process-wide (one kafka-clients jar on the classpath), so
 * threading a factory reference through every engine and worker constructor
 * would just be ceremony. Tests still inject mocks at the wrapper layer
 * ({@code KafkaConsumerWrapper}, etc.), each of which takes its {@code Raw*}
 * SPI type directly - this static is only used by production wiring.
 *
 * <p>Failure modes (both surface immediately on first call to {@link #factory()}):
 * <ul>
 *   <li>Zero impls on the classpath - misbuild or wrong tarball.</li>
 *   <li>Multiple impls on the classpath - two per-version impl modules
 *       bundled into one tarball by mistake.</li>
 * </ul>
 */
public final class KafkaSpi {

    private static final Logger log = LoggerFactory.getLogger(KafkaSpi.class);

    // Lazy-init via the holder idiom: thread-safe initialization on first call,
    // no synchronization on subsequent calls. ServiceLoader is itself not safe
    // to call repeatedly from concurrent threads in JDK 25 (it has documented
    // concurrent-iteration restrictions), so we do it once.
    private static final class Holder {
        static final KafkaClientFactory FACTORY = resolve();

        private static KafkaClientFactory resolve() {
            List<KafkaClientFactory> found = new ArrayList<>();
            for (KafkaClientFactory f : ServiceLoader.load(KafkaClientFactory.class)) {
                found.add(f);
            }
            if (found.isEmpty()) {
                throw new IllegalStateException(
                        "No dev.dimster.spi.kafka.KafkaClientFactory implementation found on the classpath. "
                                + "Each dimster build must bundle exactly one dimster-kafka-X impl module.");
            }
            if (found.size() > 1) {
                StringBuilder names = new StringBuilder();
                for (var f : found) {
                    if (names.length() > 0) names.append(", ");
                    names.append(f.getClass().getName());
                }
                throw new IllegalStateException(
                        "Multiple dev.dimster.spi.kafka.KafkaClientFactory implementations found on the classpath ("
                                + names + "). Exactly one must be bundled per dimster tarball.");
            }
            KafkaClientFactory f = found.get(0);
            Capabilities caps = f.capabilities();
            log.info("Kafka client factory bound: {} (kafka-clients version: {}, share groups: {})",
                    f.getClass().getName(),
                    caps.kafkaClientVersion(),
                    caps.shareGroupsSupported());
            return f;
        }
    }

    private KafkaSpi() {
        // static-only
    }

    /** Returns the single Kafka client factory bound on this classpath. */
    public static KafkaClientFactory factory() {
        return Holder.FACTORY;
    }

    /** Convenience for {@code factory().capabilities()}. */
    public static Capabilities capabilities() {
        return Holder.FACTORY.capabilities();
    }

    /**
     * Throws {@link UnsupportedOperationException} with a clear remediation
     * message if share groups are not supported by the bound Kafka client.
     * Call this at the entry point of any code path that requires share groups
     * (e.g. when a workload requests {@code SHARE_GROUP} consumers) to fail
     * fast with a useful error rather than failing later inside the SPI.
     */
    public static void requireShareGroups(String contextDescription) {
        Capabilities caps = capabilities();
        if (!caps.shareGroupsSupported()) {
            throw new UnsupportedOperationException(
                    contextDescription + " requires share-group support, but this dimster build is bound to "
                            + "kafka-clients " + caps.kafkaClientVersion() + " which does not support share groups. "
                            + "Build dimster against Kafka 4.2+ to run share-group workloads.");
        }
    }
}
