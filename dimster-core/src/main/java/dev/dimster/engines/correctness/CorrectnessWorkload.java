package dev.dimster.engines.correctness;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.BaseWorkload;
import dev.dimster.EngineConfig;
import dev.dimster.Scenario;
import dev.dimster.TestResult.KafkaConfigSummary;

import java.util.List;

/**
 * YAML definition for a correctness workload.
 *
 * Structure:
 *   name: ...
 *   correctnessConfig:
 *     testDurationMinutes: 5
 *     gracePeriodSeconds: 30
 *     ...
 *   baseWorkload:
 *     topics: 1
 *     partitionsPerTopic: 6
 *     ...
 *   scenarios:
 *     - name: partition-test
 *       partitionsPerTopic: [1, 6]
 */
@JsonPropertyOrder({"baseKafkaConfig", "correctnessConfig", "baseWorkload", "scenarios"})
public class CorrectnessWorkload {
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String name = "correctness";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaConfigSummary baseKafkaConfig;

    public EngineConfig engineConfig = new EngineConfig();
    public CorrectnessConfig correctnessConfig = new CorrectnessConfig();
    public BaseWorkload baseWorkload = new BaseWorkload();
    public List<Scenario> scenarios = List.of();
}
