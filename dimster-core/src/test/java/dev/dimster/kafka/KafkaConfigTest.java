package dev.dimster.kafka;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Coverage for the producer-transactions wiring on KafkaConfig:
 *
 * <ul>
 *   <li>Sweep semantics: list-valued {@code dimster.transactions.messages.per.txn}
 *       resolves correctly across scenario indices via the existing
 *       {@code resolveAtIndex} machinery (it is just a value inside
 *       producerConfig, so this test guards against accidental special-casing).</li>
 *   <li>Consumer auto-defaulting: when transactions are enabled, classic and
 *       share consumer Properties get {@code isolation.level=read_committed}
 *       unless the user explicitly sets it.</li>
 *   <li>Producer idempotence safety net: when transactions are enabled,
 *       producerProperties() injects {@code enable.idempotence=true} when the
 *       user hasn't set it, but never overrides a user-specified value.</li>
 * </ul>
 */
public class KafkaConfigTest {

    @Test
    public void messagesPerTxnSweepResolvesPerIndex() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN,
                List.of(10, 100, 1000));

        assertEquals("dimensionSize should reflect list length", 3, cfg.dimensionSize());

        assertEquals(10, cfg.resolveAtIndex(0).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN));
        assertEquals(100, cfg.resolveAtIndex(1).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN));
        assertEquals(1000, cfg.resolveAtIndex(2).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN));
    }

    @Test
    public void maxDurationMsSweepResolvesPerIndex() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS,
                List.of(50, 100, 500));

        assertEquals("dimensionSize should reflect list length", 3, cfg.dimensionSize());

        assertEquals(50, cfg.resolveAtIndex(0).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS));
        assertEquals(100, cfg.resolveAtIndex(1).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS));
        assertEquals(500, cfg.resolveAtIndex(2).producerConfig
                .get(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS));
    }

    @Test
    public void consumerIsolationLevelDefaultedToReadCommittedWhenTransactional() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);
        // Note: the user has NOT set isolation.level on either consumer config.
        cfg.classicConsumerConfig = new LinkedHashMap<>();
        cfg.shareConsumerConfig = new LinkedHashMap<>();

        assertEquals("read_committed",
                cfg.classicConsumerProperties().getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertEquals("read_committed",
                cfg.shareConsumerProperties().getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void consumerIsolationLevelLeftAloneWhenUserSetsIt() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);
        cfg.classicConsumerConfig = new LinkedHashMap<>();
        cfg.classicConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");

        assertEquals("user override must win",
                "read_uncommitted",
                cfg.classicConsumerProperties().getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void consumerIsolationLevelUntouchedWhenTransactionsDisabled() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        // dimster.transactions absent -> feature inert.
        cfg.classicConsumerConfig = new LinkedHashMap<>();

        assertNull("read_committed must NOT be auto-set when transactions are off",
                cfg.classicConsumerProperties().getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void producerIdempotenceAutoEnabledWhenTransactional() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);

        assertEquals("true",
                cfg.producerProperties().getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void producerIdempotenceUserOverrideWins() {
        var cfg = new KafkaConfig();
        cfg.producerConfig = new LinkedHashMap<>();
        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, true);
        cfg.producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        // Even though Kafka will reject this combination at producer-construction
        // time, KafkaConfig itself must not silently overwrite the user's value
        // — the explicit error from the Kafka client is more informative than
        // a quiet override.
        assertEquals("false",
                cfg.producerProperties().getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void transactionsEnabledFalseByDefault() {
        var cfg = new KafkaConfig();
        assertFalse("no producerConfig at all -> transactions disabled",
                cfg.transactionsEnabled());

        cfg.producerConfig = new LinkedHashMap<>();
        assertFalse("empty producerConfig -> transactions disabled",
                cfg.transactionsEnabled());

        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, false);
        assertFalse("explicit false -> transactions disabled",
                cfg.transactionsEnabled());

        cfg.producerConfig.put(KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED, "true");
        assertTrue("string \"true\" parses as enabled",
                cfg.transactionsEnabled());
    }
}
