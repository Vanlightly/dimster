package dev.dimster.coordinator;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Side-channel modification request for a running `run` mode benchmark.
 *
 * The CLI's `dimster run ... --modify ...` flow translates each `--<flag>
 * <value>` into one of:
 *  - an action flag (terminate / discardStats / runForMinutes) — engine-
 *    internal flips that don't change the workload itself,
 *  - a workload override (`--<workloadField> <value>`) — a key/value pair
 *    in {@link #workloadOverrides()},
 *  - a Kafka config override (`--kafkaConfig.<dotted.path> <value>`) — a
 *    flattened key/value pair in {@link #kafkaConfigOverrides()}; the dotted
 *    path's first segment selects the sub-map (commonConfig / producerConfig /
 *    classicConsumerConfig / shareConsumerConfig / topicConfig) or a top-level
 *    scalar (replicationFactor); the rest is the leaf key (which itself may
 *    contain dots — Kafka config keys are dotted strings).
 *
 * The coordinator routes the request through {@link
 * dev.dimster.engines.run.TestPointMutator} which constructs a new TestPoint
 * with the overrides applied, re-plans the topology, and executes a
 * minimally-disruptive set of worker actions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ModifyRequest(
        // ---- engine-internal action flags ----
        // True asks the engine to end the current test point as if its
        // duration had been reached: the in-flight collectStats loop sees
        // the flag, records aggregate results, breaks, and the run advances
        // to the next test point. Distinct from `dimster stop` (whole run).
        // Must be sent alone (no other field set).
        Boolean terminate,
        // True asks the engine to discard everything collected so far in
        // the current test phase. Test-phase only; cannot be combined with
        // workload/kafka overrides. May combine with runForMinutes.
        Boolean discardStats,
        // Resets the test phase remaining duration to N minutes from the
        // moment the engine processes the request. Test-phase only.
        Integer runForMinutes,
        // True asks the engine to end the current ramp-up or warm-up phase
        // and proceed straight to the next phase, without affecting the
        // recorded test phase. Rejected during the test phase — use
        // `terminate` to end the test point early. Must be sent alone.
        // The only action accepted while ramp-up is in progress.
        Boolean skipPhase,

        // ---- generalized workload field overrides ----
        // Keys are TestPoint field names in camelCase: producerRate,
        // producersPerTopic, consumerType, messageDistributor, etc.
        // Values are typed (Number / String / Boolean) — Jackson will
        // coerce on the receiving side.
        Map<String, Object> workloadOverrides,

        // ---- generalized Kafka config overrides ----
        // Keys are dotted paths *without* the leading "kafkaConfig." prefix
        // (the CLI strips it). Examples:
        //   "shareConsumerConfig.max.poll.records" -> 300
        //   "producerConfig.batch.size"            -> 65536
        //   "topicConfig.min.insync.replicas"      -> 2
        //   "replicationFactor"                    -> 3   (top-level scalar)
        Map<String, Object> kafkaConfigOverrides
) {}
