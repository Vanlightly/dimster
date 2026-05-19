package dev.dimster;

import dev.dimster.engines.drainbacklog.DrainBacklogConfig;
import dev.dimster.engines.drainbacklog.DrainBacklogDimensions;
import dev.dimster.engines.explore.ExploreDimensions;
import dev.dimster.engines.explore.ExploreWorkload.ExploreConfig;
import dev.dimster.engines.run.RunConfig;
import dev.dimster.engines.run.RunDimensions;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Expands scenarios into fully-resolved test points by merging
 * scenario overrides with base workload defaults.
 */
public class ScenarioExpander {

    public record TestPoint(
            String scenarioName,
            int scenarioIndex,
            int totalScenarios,
            String label,
            int dimensionIndex,
            int totalDimensions,
            int topics,
            int partitionsPerTopic,
            int producersPerTopic,
            int consumerGroupsPerTopic,
            int consumersPerGroup,
            int messageSize,
            double producerRate,
            MessageDistributorType messageDistributor,
            double messageDistributorSkew,
            ConsumerType consumerType,
            KafkaConfig kafkaConfig,
            int consumerProcMsMin,
            int consumerProcMsMax,
            DrainBacklogConfig drainBacklogConfig,
            ExploreConfig exploreConfig,
            RunConfig runConfig
    ) {
        public String toLogString() {
            var sb = new StringBuilder();
            sb.append("    Workload: topics=").append(topics)
              .append(", partitions=").append(partitionsPerTopic)
              .append(", producers=").append(producersPerTopic)
              .append(", consumerGroups=").append(consumerGroupsPerTopic)
              .append(", consumersPerGroup=").append(consumersPerGroup);
            sb.append("\n    Producer: rate=").append(producerRate)
              .append(", messageSize=").append(messageSize)
              .append(", messageDistributor=").append(messageDistributor);
            if (messageDistributor == MessageDistributorType.KEY_ZIPF
                    || messageDistributor == MessageDistributorType.PARTITION_ZIPF) {
                sb.append(", messageDistributorSkew=").append(messageDistributorSkew);
            }
            sb.append("\n    Consumer: type=").append(consumerType);
            if (consumerProcMsMin > 0 || consumerProcMsMax > 0) {
                sb.append(", procMs=").append(consumerProcMsMin).append("-").append(consumerProcMsMax);
            }
            if (kafkaConfig != null) {
                appendConfig(sb, "\n    Kafka: replicationFactor=", kafkaConfig.replicationFactor);
                appendMap(sb, "\n    topicConfig", kafkaConfig.topicConfig);
                appendMap(sb, "\n    producerConfig", kafkaConfig.producerConfig);
                if (consumerType == ConsumerType.CONSUMER_GROUP) {
                    appendMap(sb, "\n    classicConsumerConfig", kafkaConfig.classicConsumerConfig);
                } else if (consumerType == ConsumerType.SHARE_GROUP) {
                    appendMap(sb, "\n    shareConsumerConfig", kafkaConfig.shareConsumerConfig);
                }
            }
            if (drainBacklogConfig != null) {
                sb.append("\n    DrainBacklog: backlogSizeGB=").append(drainBacklogConfig.backlogSizeGB)
                  .append(", backlogProducerRate=").append(drainBacklogConfig.backlogProducerRate)
                  .append(", buildRate=").append(drainBacklogConfig.buildRate)
                  .append(", sampleRateMillis=").append(drainBacklogConfig.sampleRateMillis)
                  .append(", drainTimeoutMinutes=").append(drainBacklogConfig.drainTimeoutMinutes);
            }
            if (runConfig != null) {
                sb.append("\n    Run: testDurationMinutes=").append(runConfig.testDurationMinutes)
                  .append(", warmupDurationMinutes=").append(runConfig.warmupDurationMinutes)
                  .append(", rampIntervals=").append(runConfig.rampIntervals)
                  .append(", rampIntervalSeconds=").append(runConfig.rampIntervalSeconds)
                  .append(", sampleRateMillis=").append(runConfig.sampleRateMillis);
            }
            if (exploreConfig != null) {
                sb.append("\n    Explore: e2eLatencyPercentile=").append(exploreConfig.e2eLatencyPercentile)
                  .append(", e2eLatencyThresholdMs=").append(exploreConfig.e2eLatencyThresholdMs)
                  .append(", publishDelayThresholdMs=").append(exploreConfig.publishDelayThresholdMs)
                  .append(", sustainLeniency=").append(exploreConfig.sustainLeniency)
                  .append(", initialRate=").append(exploreConfig.initialRate)
                  .append(", rampWarmupSeconds=").append(exploreConfig.rampWarmupSeconds)
                  .append(", searchDurationSeconds=").append(exploreConfig.searchDurationSeconds)
                  .append(", searchIntervalSeconds=").append(exploreConfig.searchIntervalSeconds)
                  .append(", sustainDurationSeconds=").append(exploreConfig.sustainDurationSeconds)
                  .append(", searchMaxRetries=").append(exploreConfig.searchMaxRetries)
                  .append(", stabilizeRate=").append(exploreConfig.stabilizeRate)
                  .append(", stabilizeTimeoutSeconds=").append(exploreConfig.stabilizeTimeoutSeconds)
                  .append(", minViableRate=").append(exploreConfig.minViableRate)
                  .append(", perPartitionE2eMode=").append(exploreConfig.perPartitionE2eMode)
                  .append(", useDeliveryLatencyForThreshold=").append(exploreConfig.useDeliveryLatencyForThreshold);
            }
            return sb.toString();
        }

        private static void appendConfig(StringBuilder sb, String prefix, short value) {
            if (value > 0) sb.append(prefix).append(value);
        }

        private static void appendMap(StringBuilder sb, String prefix, Map<String, Object> map) {
            if (map != null && !map.isEmpty()) {
                sb.append(prefix).append(": {");
                boolean first = true;
                for (var entry : map.entrySet()) {
                    if (!first) sb.append(", ");
                    sb.append(entry.getKey()).append("=").append(entry.getValue());
                    first = false;
                }
                sb.append("}");
            }
        }
    }

    /**
     * Expand all scenarios against the base workload into test points.
     * If scenarios is null or empty, returns a single test point from the base.
     *
     * @param fileConfig optional KafkaConfig from the -k file (may be null)
     */
    public static List<TestPoint> expand(BaseWorkload base, List<Scenario> scenarios, KafkaConfig fileConfig) {
        return expand(base, scenarios, fileConfig, null, null);
    }

    /**
     * Expand all scenarios against the base workload into test points,
     * resolving per-scenario drain-backlog config overrides against the given defaults.
     *
     * @param fileConfig     optional KafkaConfig from the -k file (may be null)
     * @param drainDefaults  workload-level DrainBacklogConfig defaults (null for non-drain-backlog engines)
     */
    public static List<TestPoint> expand(BaseWorkload base, List<Scenario> scenarios,
                                          KafkaConfig fileConfig, DrainBacklogConfig drainDefaults) {
        return expand(base, scenarios, fileConfig, drainDefaults, null);
    }

    /**
     * Expand all scenarios against the base workload into test points,
     * resolving per-scenario explore config overrides against the given defaults.
     *
     * @param fileConfig      optional KafkaConfig from the -k file (may be null)
     * @param exploreDefaults workload-level ExploreConfig defaults (null for non-explore engines)
     */
    public static List<TestPoint> expand(BaseWorkload base, List<Scenario> scenarios,
                                          KafkaConfig fileConfig, ExploreConfig exploreDefaults) {
        return expand(base, scenarios, fileConfig, null, exploreDefaults);
    }

    /**
     * Expand all scenarios against the base workload into test points,
     * resolving per-scenario engine config overrides against the given defaults.
     *
     * @param fileConfig      optional KafkaConfig from the -k file (may be null)
     * @param drainDefaults   workload-level DrainBacklogConfig defaults (null for non-drain-backlog engines)
     * @param exploreDefaults workload-level ExploreConfig defaults (null for non-explore engines)
     */
    public static List<TestPoint> expand(BaseWorkload base, List<Scenario> scenarios,
                                          KafkaConfig fileConfig, DrainBacklogConfig drainDefaults,
                                          ExploreConfig exploreDefaults) {
        return expand(base, scenarios, fileConfig, drainDefaults, exploreDefaults, null);
    }

    /**
     * Expand all scenarios against the base workload into test points,
     * resolving per-scenario engine config overrides against the given defaults.
     * The run engine passes {@code runDefaults} so that per-scenario {@code runConfig}
     * sweeps populate a per-test-point {@link RunConfig}; other engines pass null.
     *
     * @param fileConfig      optional KafkaConfig from the -k file (may be null)
     * @param drainDefaults   workload-level DrainBacklogConfig defaults (null for non-drain-backlog engines)
     * @param exploreDefaults workload-level ExploreConfig defaults (null for non-explore engines)
     * @param runDefaults     workload-level RunConfig defaults (null for non-run engines)
     */
    public static List<TestPoint> expand(BaseWorkload base, List<Scenario> scenarios,
                                          KafkaConfig fileConfig, DrainBacklogConfig drainDefaults,
                                          ExploreConfig exploreDefaults, RunConfig runDefaults) {
        if (scenarios == null || scenarios.isEmpty()) {
            return List.of(fromBase(base, fileConfig, drainDefaults, exploreDefaults, runDefaults));
        }

        int effectiveTotal = scenarios.stream().mapToInt(s -> Math.max(1, s.repeat)).sum();
        List<TestPoint> points = new ArrayList<>();
        int effectiveIndex = 0;
        for (Scenario scenario : scenarios) {
            int repeats = Math.max(1, scenario.repeat);
            for (int r = 0; r < repeats; r++) {
                String effectiveName = repeats > 1
                        ? scenario.name + " (run " + (r + 1) + ")"
                        : scenario.name;
                points.addAll(expandScenario(base, scenario, effectiveName,
                        effectiveIndex, effectiveTotal, fileConfig, drainDefaults, exploreDefaults, runDefaults));
                effectiveIndex++;
            }
        }
        return points;
    }

    /**
     * Expand a single scenario into test points.
     */
    public static List<TestPoint> expandScenario(BaseWorkload base, Scenario scenario,
                                                    String effectiveName,
                                                    int scenarioIndex, int totalScenarios,
                                                    KafkaConfig fileConfig) {
        return expandScenario(base, scenario, effectiveName, scenarioIndex, totalScenarios, fileConfig, null, null);
    }

    /**
     * Expand a single scenario into test points, resolving per-scenario drain-backlog
     * config overrides against the given defaults.
     */
    public static List<TestPoint> expandScenario(BaseWorkload base, Scenario scenario,
                                                    String effectiveName,
                                                    int scenarioIndex, int totalScenarios,
                                                    KafkaConfig fileConfig,
                                                    DrainBacklogConfig drainDefaults) {
        return expandScenario(base, scenario, effectiveName, scenarioIndex, totalScenarios,
                fileConfig, drainDefaults, null);
    }

    /**
     * Expand a single scenario into test points, resolving per-scenario engine
     * config overrides (drain-backlog and explore) against the given defaults.
     * No run defaults — for run-mode sweeps, use the 9-argument overload below.
     */
    public static List<TestPoint> expandScenario(BaseWorkload base, Scenario scenario,
                                                    String effectiveName,
                                                    int scenarioIndex, int totalScenarios,
                                                    KafkaConfig fileConfig,
                                                    DrainBacklogConfig drainDefaults,
                                                    ExploreConfig exploreDefaults) {
        return expandScenario(base, scenario, effectiveName, scenarioIndex, totalScenarios,
                fileConfig, drainDefaults, exploreDefaults, null);
    }

    /**
     * Expand a single scenario into test points, resolving per-scenario engine
     * config overrides (drain-backlog, explore, and run) against the given defaults.
     * The run engine passes {@code runDefaults} to enable per-scenario {@code runConfig}
     * sweeps; other engines pass null and the resulting TestPoints carry a null runConfig.
     */
    public static List<TestPoint> expandScenario(BaseWorkload base, Scenario scenario,
                                                    String effectiveName,
                                                    int scenarioIndex, int totalScenarios,
                                                    KafkaConfig fileConfig,
                                                    DrainBacklogConfig drainDefaults,
                                                    ExploreConfig exploreDefaults,
                                                    RunConfig runDefaults) {
        // Resolve each field: null → base default as singleton list
        List<Integer> topics = resolve(scenario.topics, base.topics);
        List<Integer> partitions = resolve(scenario.partitionsPerTopic, base.partitionsPerTopic);
        List<Integer> producers = resolve(scenario.producersPerTopic, base.producersPerTopic);
        List<Integer> consumerGroups = resolve(scenario.consumerGroupsPerTopic, base.consumerGroupsPerTopic);
        List<Integer> consumers = resolve(scenario.consumersPerGroup, base.consumersPerGroup);
        List<Integer> msgSize = resolve(scenario.messageSize, base.messageSize);
        List<Double> rate = resolveDouble(scenario.producerRate, base.producerRate);
        List<MessageDistributorType> keyDist = resolve(scenario.messageDistributor, base.messageDistributor);
        List<Double> keyDistSkew = resolveDouble(scenario.messageDistributorSkew, base.messageDistributorSkew);
        List<ConsumerType> consType = resolve(scenario.consumerType, base.consumerType);
        List<Integer> procMsMin = resolve(scenario.consumerProcMsMin, base.consumerProcMsMin);
        List<Integer> procMsMax = resolve(scenario.consumerProcMsMax, base.consumerProcMsMax);
        List<String> labels = scenario.label;

        // Validate drain-backlog dimensions internally before resolving against defaults.
        // This catches mismatches between user-specified fields early — once resolve()
        // promotes missing fields to singletons, a 7-vs-6 mismatch becomes 7-vs-1 which is valid.
        DrainBacklogDimensions drainDims = scenario.drainBacklogConfig;
        if (drainDims != null) {
            drainDims.validate(effectiveName);
        }
        List<Double> drainBacklogSize = null;
        List<Integer> drainProducerRate = null;
        List<Integer> drainBuildRate = null;
        List<Integer> drainSampleRate = null;
        List<Integer> drainTimeout = null;
        if (drainDefaults != null) {
            drainBacklogSize = resolveDouble(drainDims != null ? drainDims.backlogSizeGB : null, drainDefaults.backlogSizeGB);
            drainProducerRate = resolve(drainDims != null ? drainDims.backlogProducerRate : null, drainDefaults.backlogProducerRate);
            drainBuildRate = resolve(drainDims != null ? drainDims.buildRate : null, drainDefaults.buildRate);
            drainSampleRate = resolve(drainDims != null ? drainDims.sampleRateMillis : null, drainDefaults.sampleRateMillis);
            drainTimeout = resolve(drainDims != null ? drainDims.drainTimeoutMinutes : null, drainDefaults.drainTimeoutMinutes);
        }

        // Validate explore dimensions internally before resolving against defaults.
        ExploreDimensions exploreDims = scenario.exploreConfig;
        if (exploreDims != null) {
            exploreDims.validate(effectiveName);
        }
        List<Double> explPercentile = null;
        List<Double> explThresholdMs = null;
        List<Double> explSustainLeniency = null;
        List<Double> explPublishDelay = null;
        List<Integer> explInitialRate = null;
        List<Integer> explRampInterval = null;
        List<Integer> explSearchDuration = null;
        List<Integer> explSearchInterval = null;
        List<Integer> explSearchMaxRetries = null;
        List<Integer> explSustainDuration = null;
        List<Integer> explRampWarmupSeconds = null;
        List<Integer> explStabilizeRate = null;
        List<Integer> explStabilizeTimeout = null;
        List<Double> explMinViableRate = null;
        List<Boolean> explPerPartitionE2e = null;
        List<Boolean> explUseDeliveryLatency = null;
        if (exploreDefaults != null) {
            explPercentile = resolveDouble(exploreDims != null ? exploreDims.e2eLatencyPercentile : null, exploreDefaults.e2eLatencyPercentile);
            explThresholdMs = resolveDouble(exploreDims != null ? exploreDims.e2eLatencyThresholdMs : null, exploreDefaults.e2eLatencyThresholdMs);
            explSustainLeniency = resolveDouble(exploreDims != null ? exploreDims.sustainLeniency : null, exploreDefaults.sustainLeniency);
            explPublishDelay = resolveDouble(exploreDims != null ? exploreDims.publishDelayThresholdMs : null, exploreDefaults.publishDelayThresholdMs);
            explRampInterval = resolve(exploreDims != null ? exploreDims.rampIntervalSeconds : null, exploreDefaults.rampIntervalSeconds);
            explSearchDuration = resolve(exploreDims != null ? exploreDims.searchDurationSeconds : null, exploreDefaults.searchDurationSeconds);
            explSearchInterval = resolve(exploreDims != null ? exploreDims.searchIntervalSeconds : null, exploreDefaults.searchIntervalSeconds);
            explSearchMaxRetries = resolve(exploreDims != null ? exploreDims.searchMaxRetries : null, exploreDefaults.searchMaxRetries);
            explSustainDuration = resolve(exploreDims != null ? exploreDims.sustainDurationSeconds : null, exploreDefaults.sustainDurationSeconds);
            explRampWarmupSeconds = resolve(exploreDims != null ? exploreDims.rampWarmupSeconds : null, exploreDefaults.rampWarmupSeconds);
            explStabilizeRate = resolve(exploreDims != null ? exploreDims.stabilizeRate : null, exploreDefaults.stabilizeRate);
            explStabilizeTimeout = resolve(exploreDims != null ? exploreDims.stabilizeTimeoutSeconds : null, exploreDefaults.stabilizeTimeoutSeconds);
            explInitialRate = resolve(exploreDims != null ? exploreDims.initialRate : null, exploreDefaults.initialRate);
            explMinViableRate = resolveDouble(exploreDims != null ? exploreDims.minViableRate : null, exploreDefaults.minViableRate);
            explPerPartitionE2e = resolve(exploreDims != null ? exploreDims.perPartitionE2eMode : null, exploreDefaults.perPartitionE2eMode);
            explUseDeliveryLatency = resolve(exploreDims != null ? exploreDims.useDeliveryLatencyForThreshold : null, exploreDefaults.useDeliveryLatencyForThreshold);
        }

        // Validate run-mode dimensions internally before resolving against defaults.
        RunDimensions runDims = scenario.runConfig;
        if (runDims != null) {
            runDims.validate(effectiveName);
        }
        List<Integer> runTestDuration = null;
        List<Integer> runWarmupDuration = null;
        List<Integer> runSampleRate = null;
        List<Integer> runRampIntervals = null;
        List<Integer> runRampSeconds = null;
        if (runDefaults != null) {
            runTestDuration   = resolve(runDims != null ? runDims.testDurationMinutes   : null, runDefaults.testDurationMinutes);
            runWarmupDuration = resolve(runDims != null ? runDims.warmupDurationMinutes : null, runDefaults.warmupDurationMinutes);
            runSampleRate     = resolve(runDims != null ? runDims.sampleRateMillis      : null, runDefaults.sampleRateMillis);
            runRampIntervals  = resolve(runDims != null ? runDims.rampIntervals         : null, runDefaults.rampIntervals);
            runRampSeconds    = resolve(runDims != null ? runDims.rampIntervalSeconds   : null, runDefaults.rampIntervalSeconds);
        }

        // Merge base + scenario kafkaConfig (property-level map merge)
        KafkaConfig mergedBaseKafkaConfig = KafkaConfig.merge(fileConfig, base.kafkaConfig);
        KafkaConfig finalMergedKafkaConfig = KafkaConfig.merge(mergedBaseKafkaConfig, scenario.kafkaConfig);

        // Find the number of test points (max array length among multi-element lists)
        int n = maxLength(topics, partitions, producers, consumerGroups, consumers,
                msgSize, rate, keyDist, keyDistSkew, consType, procMsMin, procMsMax);
        if (drainDefaults != null) {
            n = maxLength(n, drainBacklogSize, drainProducerRate, drainBuildRate, drainSampleRate, drainTimeout);
        }
        if (exploreDefaults != null) {
            n = maxLength(n, explPercentile, explThresholdMs, explSustainLeniency, explPublishDelay,
                    explInitialRate, explRampInterval, explSearchDuration, explSearchInterval,
                    explSearchMaxRetries, explSustainDuration, explRampWarmupSeconds,
                    explStabilizeRate, explStabilizeTimeout, explMinViableRate,
                    explPerPartitionE2e, explUseDeliveryLatency);
        }
        if (runDefaults != null) {
            n = maxLength(n, runTestDuration, runWarmupDuration, runSampleRate,
                    runRampIntervals, runRampSeconds);
        }
        if (finalMergedKafkaConfig != null && finalMergedKafkaConfig.hasDimensions()) {
            n = Math.max(n, finalMergedKafkaConfig.dimensionSize());
        }
        if (labels != null && labels.size() > 1) {
            n = Math.max(n, labels.size());
        }

        // Validate: all multi-element lists must have the same length
        validate(effectiveName, n, "topics", topics);
        validate(effectiveName, n, "partitionsPerTopic", partitions);
        validate(effectiveName, n, "producersPerTopic", producers);
        validate(effectiveName, n, "consumerGroupsPerTopic", consumerGroups);
        validate(effectiveName, n, "consumersPerGroup", consumers);
        validate(effectiveName, n, "messageSize", msgSize);
        validate(effectiveName, n, "producerRate", rate);
        validate(effectiveName, n, "messageDistributor", keyDist);
        validate(effectiveName, n, "messageDistributorSkew", keyDistSkew);
        validate(effectiveName, n, "consumerType", consType);
        validate(effectiveName, n, "consumerProcMsMin", procMsMin);
        validate(effectiveName, n, "consumerProcMsMax", procMsMax);
        if (drainDefaults != null) {
            validate(effectiveName, n, "drainBacklogConfig.backlogSizeGB", drainBacklogSize);
            validate(effectiveName, n, "drainBacklogConfig.backlogProducerRate", drainProducerRate);
            validate(effectiveName, n, "drainBacklogConfig.buildRate", drainBuildRate);
            validate(effectiveName, n, "drainBacklogConfig.sampleRateMillis", drainSampleRate);
            validate(effectiveName, n, "drainBacklogConfig.drainTimeoutMinutes", drainTimeout);
        }
        if (exploreDefaults != null) {
            validate(effectiveName, n, "exploreConfig.e2eLatencyPercentile", explPercentile);
            validate(effectiveName, n, "exploreConfig.e2eLatencyThresholdMs", explThresholdMs);
            validate(effectiveName, n, "exploreConfig.sustainLeniency", explSustainLeniency);
            validate(effectiveName, n, "exploreConfig.publishDelayThresholdMs", explPublishDelay);
            validate(effectiveName, n, "exploreConfig.initialRate", explInitialRate);
            validate(effectiveName, n, "exploreConfig.rampIntervalSeconds", explRampInterval);
            validate(effectiveName, n, "exploreConfig.searchDurationSeconds", explSearchDuration);
            validate(effectiveName, n, "exploreConfig.searchIntervalSeconds", explSearchInterval);
            validate(effectiveName, n, "exploreConfig.searchMaxRetries", explSearchMaxRetries);
            validate(effectiveName, n, "exploreConfig.sustainDurationSeconds", explSustainDuration);
            validate(effectiveName, n, "exploreConfig.rampWarmupSeconds", explRampWarmupSeconds);
            validate(effectiveName, n, "exploreConfig.stabilizeRate", explStabilizeRate);
            validate(effectiveName, n, "exploreConfig.stabilizeTimeoutSeconds", explStabilizeTimeout);
            validate(effectiveName, n, "exploreConfig.minViableRate", explMinViableRate);
            validate(effectiveName, n, "exploreConfig.perPartitionE2eMode", explPerPartitionE2e);
            validate(effectiveName, n, "exploreConfig.useDeliveryLatencyForThreshold", explUseDeliveryLatency);
        }
        if (runDefaults != null) {
            validate(effectiveName, n, "runConfig.testDurationMinutes",   runTestDuration);
            validate(effectiveName, n, "runConfig.warmupDurationMinutes", runWarmupDuration);
            validate(effectiveName, n, "runConfig.sampleRateMillis",      runSampleRate);
            validate(effectiveName, n, "runConfig.rampIntervals",         runRampIntervals);
            validate(effectiveName, n, "runConfig.rampIntervalSeconds",   runRampSeconds);
        }
        if (finalMergedKafkaConfig != null && finalMergedKafkaConfig.hasDimensions()) {
            finalMergedKafkaConfig.validateDimensions(effectiveName, n);
        }
        if (labels != null && labels.size() > 1 && labels.size() != n) {
            throw new IllegalArgumentException(
                    "Scenario '%s': label has %d elements but expected %d"
                            .formatted(effectiveName, labels.size(), n));
        }

        List<TestPoint> points = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String label = resolveLabel(base.label, effectiveName, labels, i, n);

            // Resolve kafkaConfig dimensions at this index, then merge with file config
            KafkaConfig pointConfig = finalMergedKafkaConfig != null
                    ? finalMergedKafkaConfig.resolveAtIndex(i)
                    : null;

            // Build a fully resolved DrainBacklogConfig for this test point
            DrainBacklogConfig pointDrainConfig = null;
            if (drainDefaults != null) {
                pointDrainConfig = new DrainBacklogConfig();
                pointDrainConfig.backlogSizeGB = getDouble(drainBacklogSize, i);
                pointDrainConfig.backlogProducerRate = get(drainProducerRate, i);
                pointDrainConfig.buildRate = get(drainBuildRate, i);
                pointDrainConfig.sampleRateMillis = get(drainSampleRate, i);
                pointDrainConfig.drainTimeoutMinutes = get(drainTimeout, i);
            }

            // Build a fully resolved RunConfig for this test point
            RunConfig pointRunConfig = null;
            if (runDefaults != null) {
                pointRunConfig = new RunConfig();
                pointRunConfig.testDurationMinutes   = get(runTestDuration, i);
                pointRunConfig.warmupDurationMinutes = get(runWarmupDuration, i);
                pointRunConfig.sampleRateMillis      = get(runSampleRate, i);
                pointRunConfig.rampIntervals         = get(runRampIntervals, i);
                pointRunConfig.rampIntervalSeconds   = get(runRampSeconds, i);
            }

            // Build a fully resolved ExploreConfig for this test point
            ExploreConfig pointExploreConfig = null;
            if (exploreDefaults != null) {
                pointExploreConfig = new ExploreConfig();
                pointExploreConfig.e2eLatencyPercentile = getDouble(explPercentile, i);
                pointExploreConfig.e2eLatencyThresholdMs = getDouble(explThresholdMs, i);
                pointExploreConfig.sustainLeniency = getDouble(explSustainLeniency, i);
                pointExploreConfig.publishDelayThresholdMs = getDouble(explPublishDelay, i);
                pointExploreConfig.initialRate = get(explInitialRate, i);
                pointExploreConfig.rampIntervalSeconds = get(explRampInterval, i);
                pointExploreConfig.searchDurationSeconds = get(explSearchDuration, i);
                pointExploreConfig.searchIntervalSeconds = get(explSearchInterval, i);
                pointExploreConfig.searchMaxRetries = get(explSearchMaxRetries, i);
                pointExploreConfig.sustainDurationSeconds = get(explSustainDuration, i);
                pointExploreConfig.rampWarmupSeconds = get(explRampWarmupSeconds, i);
                pointExploreConfig.stabilizeRate = get(explStabilizeRate, i);
                pointExploreConfig.stabilizeTimeoutSeconds = get(explStabilizeTimeout, i);
                pointExploreConfig.minViableRate = getDouble(explMinViableRate, i);
                pointExploreConfig.perPartitionE2eMode = get(explPerPartitionE2e, i);
                pointExploreConfig.useDeliveryLatencyForThreshold = get(explUseDeliveryLatency, i);
            }

            points.add(new TestPoint(
                    effectiveName,
                    scenarioIndex,
                    totalScenarios,
                    label,
                    i,
                    n,
                    get(topics, i),
                    get(partitions, i),
                    get(producers, i),
                    get(consumerGroups, i),
                    get(consumers, i),
                    get(msgSize, i),
                    get(rate, i),
                    get(keyDist, i),
                    getDouble(keyDistSkew, i),
                    get(consType, i),
                    pointConfig,
                    get(procMsMin, i),
                    get(procMsMax, i),
                    pointDrainConfig,
                    pointExploreConfig,
                    pointRunConfig
            ));
        }
        return points;
    }

    private static TestPoint fromBase(BaseWorkload base, KafkaConfig fileConfig,
                                       DrainBacklogConfig drainDefaults,
                                       ExploreConfig exploreDefaults,
                                       RunConfig runDefaults) {
        String label = base.label != null ? base.label : "default";
        KafkaConfig merged = KafkaConfig.merge(fileConfig, base.kafkaConfig);
        return new TestPoint(
                "default", 0, 1, label, 0, 1,
                base.topics, base.partitionsPerTopic, base.producersPerTopic,
                base.consumerGroupsPerTopic, base.consumersPerGroup,
                base.messageSize, base.producerRate,
                base.messageDistributor, base.messageDistributorSkew, base.consumerType,
                merged,
                base.consumerProcMsMin, base.consumerProcMsMax,
                drainDefaults,
                exploreDefaults,
                runDefaults
        );
    }

    private static <T> List<T> resolve(List<T> scenarioValue, T baseDefault) {
        if (scenarioValue != null && !scenarioValue.isEmpty()) {
            return scenarioValue;
        }
        return List.of(baseDefault);
    }

    /** Double-typed variant of {@link #resolve} for fields like backlogSizeGB. */
    private static List<Double> resolveDouble(List<Double> scenarioValue, double baseDefault) {
        if (scenarioValue != null && !scenarioValue.isEmpty()) {
            return scenarioValue;
        }
        return List.of(baseDefault);
    }

    private static <T> T get(List<T> list, int index) {
        // Singletons repeat for all test points
        if (list.size() == 1) return list.getFirst();
        return list.get(index);
    }

    /** Double-typed variant of {@link #get}. */
    private static double getDouble(List<Double> list, int index) {
        if (list.size() == 1) return list.getFirst();
        return list.get(index);
    }

    @SafeVarargs
    private static int maxLength(List<?>... lists) {
        int max = 1;
        for (List<?> list : lists) {
            max = Math.max(max, list.size());
        }
        return max;
    }

    /** Compute max length starting from an existing value (for composing with drain-backlog dimensions). */
    private static int maxLength(int current, List<?>... lists) {
        int max = current;
        for (List<?> list : lists) {
            max = Math.max(max, list.size());
        }
        return max;
    }

    private static void validate(String scenarioName, int expectedSize, String fieldName, List<?> list) {
        if (list.size() > 1 && list.size() != expectedSize) {
            throw new IllegalArgumentException(
                    "Scenario '%s': %s has %d elements but expected %d (all arrays must be the same length)"
                            .formatted(scenarioName, fieldName, list.size(), expectedSize));
        }
    }

    private static String resolveLabel(String baseLabel, String scenarioName,
                                        List<String> scenarioLabels, int index, int total) {
        String pointLabel = (scenarioLabels != null && !scenarioLabels.isEmpty())
                ? get(scenarioLabels, index)
                : (total > 1 ? scenarioName + " #" + (index + 1) : scenarioName);

        if (baseLabel != null && !baseLabel.isEmpty()) {
            return baseLabel + " - " + pointLabel;
        }
        return pointLabel;
    }
}
