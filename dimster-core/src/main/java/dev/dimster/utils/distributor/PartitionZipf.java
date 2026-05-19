package dev.dimster.utils.distributor;

/**
 * Forces an explicit partition number drawn from a Zipf distribution.
 * Partition 0 is always the hottest; partition (n-1) is the coldest. The
 * record is sent with a null key — Kafka's partitioner is bypassed via
 * the {@code ProducerRecord(topic, partition, key, value)} constructor.
 *
 * <p>Use this when you want a deterministic, maximally-skewed load profile
 * (e.g. for stressing share-group fetcher fairness or per-partition
 * back-pressure handling). For a softer skew that still hashes through
 * the partitioner, use {@link KeyZipf}.
 */
public final class PartitionZipf implements MessageDistributor {

    private final ZipfSampler sampler;

    public PartitionZipf(int partitionsPerTopic, double skew) {
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException(
                    "partitionsPerTopic must be >= 1 for PARTITION_ZIPF, got "
                            + partitionsPerTopic);
        }
        this.sampler = new ZipfSampler(partitionsPerTopic, skew);
    }

    @Override
    public Target next() {
        // sampler returns [1, n]; partition numbers are 0-based.
        return Target.ofPartition(sampler.sample() - 1);
    }
}
