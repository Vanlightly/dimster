package dev.dimster.worker.topology;

import java.util.List;

import org.apache.kafka.common.TopicPartition;

/**
 * One virtual consumer in a {@link VirtualTopology}: a single Kafka consumer
 * instance that the planner has decided should be hosted by {@code worker} and
 * subscribe to {@code topic} as part of {@code consumerGroup}.
 *
 * <h2>Static partition assignment</h2>
 * <p>{@link #staticPartitions} is the set of {@link TopicPartition}s the
 * consumer should manually assign at startup (bypassing group rebalance).
 * <ul>
 *   <li>An empty list means dynamic assignment — the consumer joins the group
 *       normally and lets Kafka's group coordinator distribute partitions.</li>
 *   <li>A non-empty list means manual assignment, used in correctness mode for
 *       classic consumers so that partition-to-consumer mapping is stable and
 *       reproducible across the test (no rebalance noise).</li>
 * </ul>
 *
 * <h2>Identity</h2>
 * <p>{@link #consumerIdx} is the consumer's stable position
 * {@code [0, consumersPerGroup)} within its (topic, group). It is used to
 * derive a deterministic name and to address the consumer in the
 * partition-distribution algorithm.
 */
public record VirtualConsumer(
        String worker,
        String topic,
        String consumerGroup,
        int consumerIdx,
        List<TopicPartition> staticPartitions) {

    public VirtualConsumer {
        if (worker == null || worker.isEmpty()) {
            throw new IllegalArgumentException("worker must be non-empty");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic must be non-empty");
        }
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            throw new IllegalArgumentException("consumerGroup must be non-empty");
        }
        if (consumerIdx < 0) {
            throw new IllegalArgumentException("consumerIdx must be >= 0");
        }
        // Make the partition list immutable from outside; List.copyOf rejects nulls.
        staticPartitions = staticPartitions == null
                ? List.of()
                : List.copyOf(staticPartitions);
    }

    public String name() {
        return "consumer-" + topic + "-" + consumerGroup + "-i" + consumerIdx;
    }

    /** True if this consumer should manually assign partitions instead of joining the group. */
    public boolean hasStaticPartitions() {
        return !staticPartitions.isEmpty();
    }
}
