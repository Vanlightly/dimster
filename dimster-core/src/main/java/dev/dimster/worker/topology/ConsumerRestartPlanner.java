package dev.dimster.worker.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Selects concrete consumers for an in-place restart from an active topology.
 *
 * <p>Both run and correctness modes use the same planner so count capping,
 * selection without replacement, and recreation settings cannot drift. The
 * caller supplies the source of randomness: run mode uses runtime randomness,
 * while correctness mode uses a reproducible per-test-point seed.
 */
public final class ConsumerRestartPlanner {

    private ConsumerRestartPlanner() {}

    /**
     * The exact consumer set to send to workers and the stable names selected
     * for logging and result-file auditing.
     */
    public record Plan(ConsumerSet consumerSet, List<String> selectedNames) {}

    /**
     * Select up to {@code requestedCount} consumers uniformly without
     * replacement. Requests larger than the topology restart every consumer;
     * zero is an explicit no-op.
     */
    public static Plan plan(VirtualTopology topology, int requestedCount, Random random) {
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(random, "random");
        if (requestedCount < 0) {
            throw new IllegalArgumentException("restartConsumersCount must be >= 0, got "
                    + requestedCount);
        }

        List<VirtualConsumer> candidates = topology.consumers();
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("no consumers to restart "
                    + "(topology has 0 consumers)");
        }

        int actualCount = Math.min(requestedCount, candidates.size());
        List<VirtualConsumer> selected;
        if (actualCount == 0) {
            selected = List.of();
        } else if (actualCount == candidates.size()) {
            // Preserve topology order when there is no random choice to make.
            selected = List.copyOf(candidates);
        } else {
            List<VirtualConsumer> shuffled = new ArrayList<>(candidates);
            Collections.shuffle(shuffled, random);
            selected = List.copyOf(shuffled.subList(0, actualCount));
        }

        ConsumerSet consumerSet = new ConsumerSet(selected, topology.consumerType(),
                topology.consumerProcMsMin(), topology.consumerProcMsMax(),
                topology.consumerProcParallelism(), topology.consumerProcMode(),
                topology.consumerProcCapacity());
        List<String> selectedNames = selected.stream().map(VirtualConsumer::name).toList();
        return new Plan(consumerSet, selectedNames);
    }
}
