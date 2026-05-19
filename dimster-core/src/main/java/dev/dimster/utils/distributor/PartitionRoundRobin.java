package dev.dimster.utils.distributor;

/**
 * Cycles through partitions {@code [0, partitionsPerTopic)} in round-robin
 * order, bypassing Kafka's partitioner entirely. Produces a perfectly
 * uniform load across all partitions — the complement of
 * {@link PartitionZipf} which concentrates load on low-numbered partitions.
 *
 * <p>Multiple producer threads share one instance and race on
 * {@code currentPartition} — the race is benign (threads may see slightly
 * out-of-order partitions), so we don't synchronize. This matches the
 * threading model of {@link KeyRoundRobin}.
 */
public final class PartitionRoundRobin implements MessageDistributor {

    private final int partitionsPerTopic;
    private int currentPartition = -1;

    public PartitionRoundRobin(int partitionsPerTopic) {
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException(
                    "partitionsPerTopic must be >= 1 for PARTITION_ROUND_ROBIN, got "
                            + partitionsPerTopic);
        }
        this.partitionsPerTopic = partitionsPerTopic;
    }

    @Override
    public Target next() {
        int p = ++currentPartition;
        if (p >= partitionsPerTopic) {
            currentPartition = 0;
            p = 0;
        }
        return Target.ofPartition(p);
    }
}
