package dev.dimster.worker.topology;

import java.util.List;

import dev.dimster.ConsumerType;

/**
 * The portion of a {@link VirtualTopology} that goes to one worker for
 * consumer creation, plus the few uniform parameters that apply to every
 * consumer in the topology (consumer type and simulated processing-time
 * range).
 *
 * <p>Bundled into a single record so that {@code Worker.createConsumers} has
 * exactly one argument and so the same wire shape is used by both
 * {@code LocalWorker} (in-process) and {@code RemoteWorkerClient} (HTTP).
 */
public record ConsumerSet(
        List<VirtualConsumer> consumers,
        ConsumerType consumerType,
        int consumerProcMsMin,
        int consumerProcMsMax,
        int consumerProcParallelism) {

    public ConsumerSet {
        consumers = List.copyOf(consumers);
    }

    public boolean isEmpty() {
        return consumers.isEmpty();
    }
}
