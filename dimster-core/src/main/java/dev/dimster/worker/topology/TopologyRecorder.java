package dev.dimster.worker.topology;

/**
 * Sink that engines call once per test point with the {@link VirtualTopology}
 * the planner produced. Implementations decide what to do with it — typically
 * append to an in-memory list and rewrite a JSON file on disk so the topology
 * for every test point is persisted as part of the run's results.
 *
 * <p>Lives in this package so it can be passed to {@code BaseEngine} without
 * dragging the engine layer into the {@code BenchmarkRunner} package or vice
 * versa.
 */
@FunctionalInterface
public interface TopologyRecorder {

    /** No-op recorder, used when the engine does not need topology persistence (e.g. unit tests). */
    TopologyRecorder NO_OP = (label, topology) -> {};

    /**
     * Record the topology for one test point.
     *
     * @param testPointLabel  the {@code TestPoint.label()} (or scenario name);
     *                        identifies which test point produced the topology
     * @param topology        the planned topology, immutable
     */
    void record(String testPointLabel, VirtualTopology topology);
}
