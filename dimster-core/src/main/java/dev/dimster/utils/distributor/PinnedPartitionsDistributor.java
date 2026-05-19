package dev.dimster.utils.distributor;

/**
 * Marker distributor for {@link MessageDistributorType#PINNED_PARTITIONS}. Returns
 * a null key and a null partition on every {@link #next()} — the actual
 * partition selection is performed by {@code KafkaProducerWrapper} using the
 * pinned partition list assigned to each producer by {@code TopologyPlanner}.
 *
 * <p>Existing as its own type (rather than reusing {@link NoKeyDistributor})
 * keeps the {@link MessageDistributor#of} factory total over
 * {@link MessageDistributorType} and makes the relationship between the enum value
 * and the no-op runtime behavior explicit.
 */
public final class PinnedPartitionsDistributor implements MessageDistributor {

    @Override
    public Target next() {
        return Target.NULL_KEY;
    }
}
