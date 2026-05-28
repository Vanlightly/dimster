package dev.dimster.worker.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dev.dimster.ConsumerType;
import dev.dimster.utils.distributor.MessageDistributorType;

/**
 * The complete, immutable description of a single test point's load topology:
 * which workers run which producers and consumers, which topics each produces
 * to or consumes from, and the exact per-instance partition assignment.
 *
 * <p>This record is the single source of truth for "where things run" — it
 * subsumes the worker-level decisions previously made by
 * {@code WorkerAssignmentPlanner} and the per-instance partition-pinning
 * decisions previously made inside {@code LocalWorker} (the per-topic
 * partition counter in {@code createProducers} and
 * {@code computeStaticPartitionAssignments} for consumers). With everything
 * pre-computed, both {@code LocalWorker} and {@code RemoteWorkerClient}
 * become thin executors of the topology, and the planner can be unit-tested
 * end-to-end against this single output object.
 *
 * <p>Built by {@code TopologyPlanner.plan(TopologySpec)}.
 *
 * @param topicPartitionCounts        topic -> partition count, the cluster-side
 *                                    facts the rest of the topology was
 *                                    computed against
 * @param producers                   every virtual producer in the topology;
 *                                    use {@link #producersForWorker} to get the
 *                                    subset for a given worker
 * @param consumers                   every virtual consumer in the topology;
 *                                    use {@link #consumersForWorker} to get the
 *                                    subset for a given worker
 * @param consumerType                CONSUMER_GROUP or SHARE_GROUP — uniform across
 *                                    the topology in current usage
 * @param consumerProcMsMin           minimum simulated per-record processing
 *                                    delay applied to every consumer
 * @param consumerProcMsMax           maximum simulated per-record processing
 *                                    delay applied to every consumer
 * @param consumerProcParallelism     processing concurrency mode: 1=sequential,
 *                                    >1=constrained concurrency, -1=full parallelism
 * @param messageDistributorType          per-message key distributor used by every
 *                                    producer (the same instance is shared
 *                                    across producers; per-producer pinning is
 *                                    expressed via {@link VirtualProducer#pinnedPartitions})
 * @param messageDistributorSkew          Zipf skew parameter for the key
 *                                    distributor (ignored by non-Zipf types)
 * @param activeProducerWorkerCount   number of workers actually hosting at
 *                                    least one producer; used by callers to
 *                                    divide the global publish rate
 */
public record VirtualTopology(
        Map<String, Integer> topicPartitionCounts,
        List<VirtualProducer> producers,
        List<VirtualConsumer> consumers,
        ConsumerType consumerType,
        int consumerProcMsMin,
        int consumerProcMsMax,
        int consumerProcParallelism,
        MessageDistributorType messageDistributorType,
        double messageDistributorSkew,
        int activeProducerWorkerCount) {

    public VirtualTopology {
        // Preserve insertion order (Map.copyOf does not). Topic order matters for several
        // consumers — engines feed it into createTopic calls and the planner relies on it
        // when matching producers/consumers to topics.
        topicPartitionCounts = Collections.unmodifiableMap(new LinkedHashMap<>(topicPartitionCounts));
        producers = List.copyOf(producers);
        consumers = List.copyOf(consumers);
    }

    /** All distinct workers referenced anywhere in the topology, in insertion order. */
    public Set<String> workers() {
        Set<String> all = new LinkedHashSet<>();
        for (VirtualProducer p : producers) all.add(p.worker());
        for (VirtualConsumer c : consumers) all.add(c.worker());
        return all;
    }

    /** The subset of producers hosted by {@code worker}, preserving topology order. */
    public List<VirtualProducer> producersForWorker(String worker) {
        List<VirtualProducer> out = new ArrayList<>();
        for (VirtualProducer p : producers) {
            if (p.worker().equals(worker)) out.add(p);
        }
        return out;
    }

    /** The subset of consumers hosted by {@code worker}, preserving topology order. */
    public List<VirtualConsumer> consumersForWorker(String worker) {
        List<VirtualConsumer> out = new ArrayList<>();
        for (VirtualConsumer c : consumers) {
            if (c.worker().equals(worker)) out.add(c);
        }
        return out;
    }

    /**
     * Returns the per-worker {@link ConsumerSet} ready to hand to
     * {@code Worker.createConsumers}: the consumer subset for {@code worker}
     * combined with the topology-wide consumer-type and processing-time
     * settings.
     */
    public ConsumerSet consumerSetForWorker(String worker) {
        return new ConsumerSet(consumersForWorker(worker), consumerType, consumerProcMsMin, consumerProcMsMax, consumerProcParallelism);
    }
}
