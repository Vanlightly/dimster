package dev.dimster.worker.topology;

import java.util.Arrays;

/**
 * One virtual producer in a {@link VirtualTopology}: a single Kafka producer
 * instance that the planner has decided should be hosted by {@code worker} and
 * send to {@code topic}, with an optional set of statically pinned partitions.
 *
 * <h2>Pinning semantics</h2>
 * <p>The {@link #pinnedPartitions} array describes producer-level partition
 * pinning, computed once by {@code TopologyPlanner}:
 * <ul>
 *   <li>An empty array means "no pinning" — the producer's
 *       {@code KafkaProducerWrapper} will defer to either the
 *       {@code MessageDistributor}'s explicit partition (e.g. PARTITION_ZIPF) or
 *       Kafka's configured partitioner.</li>
 *   <li>A single-element array {@code [P]} means the producer always sends to
 *       partition {@code P}. This is the correctness-mode case (one producer
 *       per partition).</li>
 *   <li>A multi-element array means the producer round-robins through that
 *       subset of partitions for every send. This is the
 *       {@code PINNED_PARTITIONS} key-distributor case, where the planner has
 *       split a topic's partitions evenly across its producers.</li>
 * </ul>
 * The per-message round-robin cursor lives on the {@code KafkaProducerWrapper}
 * (per-instance, no cross-producer sharing); this record only carries the
 * static assignment.
 *
 * <h2>Identity</h2>
 * <p>{@link #producerIdx} is the producer's stable position
 * {@code [0, producersPerTopic)} within its topic. It is used to build a
 * stable name and (for transactional producers) a stable {@code transactional.id}
 * — both decoupled from partition assignment so that pinning can change between
 * scenarios without altering producer identity.
 *
 * @param worker            the worker hosting this producer
 * @param topic             the topic this producer publishes to
 * @param producerIdx       stable index within (topic, all producers for topic)
 * @param pinnedPartitions  partitions this producer round-robins through;
 *                          empty array means no pinning
 */
public record VirtualProducer(
        String worker,
        String topic,
        int producerIdx,
        int[] pinnedPartitions) {

    public VirtualProducer {
        if (worker == null || worker.isEmpty()) {
            throw new IllegalArgumentException("worker must be non-empty");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic must be non-empty");
        }
        if (producerIdx < 0) {
            throw new IllegalArgumentException("producerIdx must be >= 0");
        }
        // Defensive copy keeps the record's int[] immutable from the caller's
        // perspective, matching the value-record contract.
        pinnedPartitions = pinnedPartitions == null
                ? new int[0]
                : Arrays.copyOf(pinnedPartitions, pinnedPartitions.length);
    }

    /** Stable client name used in logs and Prometheus labels. */
    public String name() {
        return "producer-" + topic + "-i" + producerIdx;
    }

    /**
     * Stable transactional.id for this producer slot. Index-based (not
     * partition-based) so that pinning can change between scenarios without
     * fencing the producer instance — the underlying KafkaProducer needs a
     * unique id stable for its lifetime, not for its partition assignment.
     */
    public String transactionalId(String hostname) {
        return "dimster-" + hostname + "-" + topic + "-i" + producerIdx;
    }

    /** True if this producer has no pinning (defers to MessageDistributor / Kafka partitioner). */
    public boolean hasPinning() {
        return pinnedPartitions.length > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualProducer that)) return false;
        return producerIdx == that.producerIdx
                && worker.equals(that.worker)
                && topic.equals(that.topic)
                && Arrays.equals(pinnedPartitions, that.pinnedPartitions);
    }

    @Override
    public int hashCode() {
        int h = worker.hashCode();
        h = 31 * h + topic.hashCode();
        h = 31 * h + producerIdx;
        h = 31 * h + Arrays.hashCode(pinnedPartitions);
        return h;
    }

    @Override
    public String toString() {
        return "VirtualProducer{worker=" + worker
                + ", topic=" + topic
                + ", idx=" + producerIdx
                + ", pinned=" + Arrays.toString(pinnedPartitions) + "}";
    }
}
