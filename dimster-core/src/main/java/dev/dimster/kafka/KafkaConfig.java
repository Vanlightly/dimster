package dev.dimster.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConfig {

    /**
     * Producer-side dimster knob: when set to {@code true}, each producer is
     * driven in transactional mode (initTransactions / beginTransaction /
     * commitTransaction loop, with a unique transactional.id assigned per
     * producer instance by {@code LocalWorker.createProducers}).
     *
     * <p>Lives inside {@code producerConfig} alongside Kafka client properties
     * — same convention as the consumer-side {@code dimster.commit.mode} key
     * read by {@code KafkaConsumerWrapper}. The Kafka client will log an
     * "unknown config" warning for this key, which is the accepted pattern.
     */
    public static final String DIMSTER_TRANSACTIONS_ENABLED = "dimster.transactions";

    /**
     * Producer-side dimster knob: number of messages to send per transaction
     * before calling commitTransaction. Optional — at least one of this and
     * {@link #DIMSTER_TRANSACTIONS_MAX_DURATION_MS} must be set when
     * {@link #DIMSTER_TRANSACTIONS_ENABLED} is true. Sweepable across
     * scenarios as a list value, e.g. {@code [10, 100, 1000]}.
     */
    public static final String DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN =
            "dimster.transactions.messages.per.txn";

    /**
     * Producer-side dimster knob: max wall-clock duration (ms) of a transaction
     * before commit is forced. Combined with
     * {@link #DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN} via OR semantics — commit
     * fires when whichever threshold is reached first. Optional; at least one
     * of the two triggers must be set when transactions are enabled.
     *
     * <p>The clock starts at {@code beginTransaction()}, so this is the actual
     * transaction window, not idle wait time. The check is evaluated only
     * inside the producer send loop, so at very low send rates this behaves as
     * "max time between commits while sending actively" — an idle producer
     * holds an open txn until its next send arrives. Acceptable for the
     * benchmarking workloads dimster targets.
     */
    public static final String DIMSTER_TRANSACTIONS_MAX_DURATION_MS =
            "dimster.transactions.max.txn.duration.ms";

    public String name;
    public short replicationFactor;
    public Map<String, Object> topicConfig;
    public Map<String, Object> commonConfig;
    public Map<String, Object> producerConfig;
    public Map<String, Object> classicConsumerConfig;
    public Map<String, Object> shareConsumerConfig;

    public Properties commonProperties() {
        var props = new Properties();
        putAll(props, commonConfig);
        return props;
    }

    public Properties producerProperties() {
        var props = new Properties();
        putAll(props, commonConfig);
        putAll(props, producerConfig);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // When transactions are enabled, idempotence is mandatory. Recent Kafka
        // clients auto-enable it for transactional producers, but we set it
        // explicitly to keep the produced Properties self-describing in logs.
        if (transactionsEnabled() && !props.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        }
        return props;
    }

    public Properties classicConsumerProperties() {
        var props = new Properties();
        putAll(props, commonConfig);
        putAll(props, classicConsumerConfig);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyTransactionalConsumerDefaults(props, classicConsumerConfig);
        return props;
    }

    public Properties shareConsumerProperties() {
        var props = new Properties();
        putAll(props, commonConfig);
        putAll(props, shareConsumerConfig);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        applyTransactionalConsumerDefaults(props, shareConsumerConfig);
        return props;
    }

    /**
     * When the producer is transactional, default the consumer to
     * {@code read_committed} so that uncommitted/aborted records are not
     * surfaced — otherwise the workload would silently report end-to-end
     * latency on records that never become visible to a real EOS reader.
     * The user can opt out by explicitly setting {@code isolation.level} in
     * the consumer config map.
     */
    private void applyTransactionalConsumerDefaults(Properties props,
                                                     Map<String, Object> userConsumerConfig) {
        if (!transactionsEnabled()) return;
        boolean userSetIsolation = userConsumerConfig != null
                && userConsumerConfig.containsKey(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        if (!userSetIsolation) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
    }

    /** True when {@code dimster.transactions=true} is set in producerConfig. */
    public boolean transactionsEnabled() {
        if (producerConfig == null) return false;
        Object val = producerConfig.get(DIMSTER_TRANSACTIONS_ENABLED);
        return val != null && Boolean.parseBoolean(val.toString());
    }

    public Map<String, String> topicProperties() {
        var props = new HashMap<String, String>();
        if (topicConfig != null) {
            topicConfig.forEach((k, v) -> props.put(k, v.toString()));
        }
        return props;
    }

    private static void putAll(Properties props, Map<String, Object> map) {
        if (map != null) {
            map.forEach((k, v) -> props.put(k, v.toString()));
        }
    }

    /**
     * Merge two KafkaConfigs at the property level. Override keys take precedence;
     * base keys not present in override are preserved. Returns a new instance.
     */
    public static KafkaConfig merge(KafkaConfig base, KafkaConfig override) {
        if (base == null) return override;
        if (override == null) return base;

        var merged = new KafkaConfig();
        merged.name = (override.name != null) ? override.name : base.name;
        merged.replicationFactor = (override.replicationFactor > 0) ? override.replicationFactor : base.replicationFactor;
        merged.topicConfig = mergeMaps(base.topicConfig, override.topicConfig);
        merged.commonConfig = mergeMaps(base.commonConfig, override.commonConfig);
        merged.producerConfig = mergeMaps(base.producerConfig, override.producerConfig);
        merged.classicConsumerConfig = mergeMaps(base.classicConsumerConfig, override.classicConsumerConfig);
        merged.shareConsumerConfig = mergeMaps(base.shareConsumerConfig, override.shareConsumerConfig);
        return merged;
    }

    private static Map<String, Object> mergeMaps(Map<String, Object> base, Map<String, Object> override) {
        if (base == null || base.isEmpty()) return override;
        if (override == null || override.isEmpty()) return base;
        var merged = new LinkedHashMap<>(base);
        merged.putAll(override);
        return merged;
    }

    /**
     * Returns the dimension size: max list length across all config map values.
     * Returns 1 if all values are scalars or all maps are null.
     */
    public int dimensionSize() {
        int max = 1;
        max = Math.max(max, maxListSize(topicConfig));
        max = Math.max(max, maxListSize(commonConfig));
        max = Math.max(max, maxListSize(producerConfig));
        max = Math.max(max, maxListSize(classicConsumerConfig));
        max = Math.max(max, maxListSize(shareConsumerConfig));
        return max;
    }

    /**
     * Returns true if any config map contains list values (dimensions).
     */
    public boolean hasDimensions() {
        return dimensionSize() > 1;
    }

    /**
     * Validate that all list values across all config maps have the expected size.
     */
    public void validateDimensions(String scenarioName, int expectedSize) {
        validateMapDimensions(scenarioName, "topicConfig", topicConfig, expectedSize);
        validateMapDimensions(scenarioName, "commonConfig", commonConfig, expectedSize);
        validateMapDimensions(scenarioName, "producerConfig", producerConfig, expectedSize);
        validateMapDimensions(scenarioName, "classicConsumerConfig", classicConsumerConfig, expectedSize);
        validateMapDimensions(scenarioName, "shareConsumerConfig", shareConsumerConfig, expectedSize);
    }

    /**
     * Resolve list values at the given index, returning a new KafkaConfig with all scalar values.
     */
    public KafkaConfig resolveAtIndex(int index) {
        var resolved = new KafkaConfig();
        resolved.name = name;
        resolved.replicationFactor = replicationFactor;
        resolved.topicConfig = resolveMapAtIndex(topicConfig, index);
        resolved.commonConfig = resolveMapAtIndex(commonConfig, index);
        resolved.producerConfig = resolveMapAtIndex(producerConfig, index);
        resolved.classicConsumerConfig = resolveMapAtIndex(classicConsumerConfig, index);
        resolved.shareConsumerConfig = resolveMapAtIndex(shareConsumerConfig, index);
        return resolved;
    }

    private static Map<String, Object> resolveMapAtIndex(Map<String, Object> map, int index) {
        if (map == null) return null;
        var resolved = new LinkedHashMap<String, Object>();
        for (var entry : map.entrySet()) {
            Object val = entry.getValue();
            if (val instanceof List<?> list) {
                resolved.put(entry.getKey(), list.size() == 1 ? list.getFirst() : list.get(index));
            } else {
                resolved.put(entry.getKey(), val);
            }
        }
        return resolved;
    }

    private static int maxListSize(Map<String, Object> map) {
        if (map == null) return 0;
        int max = 0;
        for (var val : map.values()) {
            if (val instanceof List<?> list) {
                max = Math.max(max, list.size());
            }
        }
        return max;
    }

    private static void validateMapDimensions(String scenarioName, String configName,
                                               Map<String, Object> map, int expectedSize) {
        if (map == null) return;
        for (var entry : map.entrySet()) {
            if (entry.getValue() instanceof List<?> list && list.size() > 1 && list.size() != expectedSize) {
                throw new IllegalArgumentException(
                        "Scenario '%s': kafkaConfig.%s.%s has %d elements but expected %d"
                                .formatted(scenarioName, configName, entry.getKey(), list.size(), expectedSize));
            }
        }
    }
}
