package dev.dimster.engines.correctness;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.ConsumerRestartPlanner;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualTopology;

/**
 * Owns the correctness-mode consumer restart action and its lifecycle gate.
 *
 * <p>HTTP modify calls arrive on coordinator threads while the correctness
 * engine advances test phases on its benchmark thread. Keeping both the phase
 * transitions and worker dispatch synchronized here guarantees that teardown
 * cannot overtake a restart already accepted for the measured phase.
 */
final class CorrectnessConsumerRestartController {

    private static final Logger log = LoggerFactory.getLogger(
            CorrectnessConsumerRestartController.class);

    private final Worker worker;
    private final List<CorrectnessResult.ConsumerRestartEvent> events = new ArrayList<>();

    private VirtualTopology topology;
    private Random random = new Random(0);
    private boolean restartWindowOpen;
    private boolean restartDispatchFailed;

    CorrectnessConsumerRestartController(Worker worker) {
        this.worker = worker;
    }

    /** Reset deterministic selection and audit state for a newly planned test point. */
    synchronized void prepareTest(VirtualTopology topology, long seed) {
        this.topology = topology;
        this.random = new Random(seed);
        this.events.clear();
        this.restartWindowOpen = false;
        this.restartDispatchFailed = false;
    }

    /** Open the action window after load and measurement statistics have started. */
    synchronized void openWindow() {
        restartWindowOpen = true;
    }

    /**
     * Close the action window before grace-period or teardown work starts.
     * This waits for any synchronized worker dispatch already in progress.
     */
    synchronized void closeWindow() {
        restartWindowOpen = false;
    }

    /** Apply the one live action supported by correctness mode. */
    synchronized ModifyResult applyModification(ModifyRequest request) {
        if (request == null) {
            return ModifyResult.rejected("empty modify request");
        }

        Integer requestedCount = request.restartConsumersCount();
        if (requestedCount == null) {
            return ModifyResult.rejected("correctness mode only supports restartConsumersCount");
        }
        if (hasAnyOtherAction(request)) {
            return ModifyResult.rejected("restartConsumersCount must be sent alone in correctness mode");
        }
        if (requestedCount < 0) {
            return ModifyResult.rejected("restartConsumersCount must be >= 0, got " + requestedCount);
        }
        if (!restartWindowOpen || topology == null) {
            return ModifyResult.rejected("restartConsumersCount is only accepted during the measured "
                    + "CONCURRENT correctness phase");
        }
        if (topology.consumers().stream().anyMatch(VirtualConsumer::hasStaticPartitions)) {
            return ModifyResult.rejected("correctness consumers use manual partition assignment; "
                    + "set correctnessConfig.staticConsumerAssignment=false to exercise a group rebalance");
        }

        ConsumerRestartPlanner.Plan plan;
        try {
            plan = ConsumerRestartPlanner.plan(topology, requestedCount, random);
        } catch (IllegalArgumentException e) {
            return ModifyResult.rejected(e.getMessage());
        }

        List<String> selectedNames = plan.selectedNames();
        log.info("CORRECTNESS MODIFY selected consumers for restart: {}", selectedNames);
        if (plan.consumerSet().isEmpty()) {
            String message = "restartConsumersCount=0 is a no-op";
            events.add(new CorrectnessResult.ConsumerRestartEvent(
                    Instant.now().toString(), requestedCount, selectedNames, true, message));
            log.info("CORRECTNESS MODIFY applied: {}", message);
            return ModifyResult.ok(message);
        }

        try {
            worker.restartConsumers(plan.consumerSet());
            String message = "restarted " + selectedNames.size() + " consumers";
            events.add(new CorrectnessResult.ConsumerRestartEvent(
                    Instant.now().toString(), requestedCount, selectedNames, true, message));
            log.info("CORRECTNESS MODIFY applied: {}", message);
            return ModifyResult.ok(message);
        } catch (Exception e) {
            restartDispatchFailed = true;
            String detail = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            String message = "consumer restart failed after selection: " + detail;
            events.add(new CorrectnessResult.ConsumerRestartEvent(
                    Instant.now().toString(), requestedCount, selectedNames, false, message));
            log.error("CORRECTNESS MODIFY rejected after dispatch: {}", message, e);
            return ModifyResult.rejected(message);
        }
    }

    /** Snapshot events after the restart window has closed. */
    synchronized List<CorrectnessResult.ConsumerRestartEvent> events() {
        return List.copyOf(events);
    }

    /** A failed fault injection invalidates the current correctness comparison. */
    synchronized boolean restartDispatchFailed() {
        return restartDispatchFailed;
    }

    /** Attach this test point's audit trail and fault-injection status. */
    synchronized void populateOutcome(CorrectnessResult.TestOutcome outcome) {
        outcome.consumerRestarts = new ArrayList<>(events);
        if (restartDispatchFailed) {
            // A failed or partially-applied fault injection makes the result
            // unsuitable for comparing group protocols, regardless of the
            // message-level checks reported by workers.
            outcome.correctness.passed = false;
        }
    }

    private boolean hasAnyOtherAction(ModifyRequest request) {
        return Boolean.TRUE.equals(request.terminate())
                || Boolean.TRUE.equals(request.discardStats())
                || Boolean.TRUE.equals(request.skipPhase())
                || request.runForMinutes() != null
                || request.restartProducersCount() != null
                || (request.workloadOverrides() != null && !request.workloadOverrides().isEmpty())
                || (request.kafkaConfigOverrides() != null && !request.kafkaConfigOverrides().isEmpty());
    }
}
