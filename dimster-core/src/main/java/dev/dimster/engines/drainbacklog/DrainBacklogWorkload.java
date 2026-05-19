package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.BaseWorkload;
import dev.dimster.EngineConfig;
import dev.dimster.Scenario;
import dev.dimster.TestResult.KafkaConfigSummary;

import java.util.List;

/**
 * YAML definition for a drain-backlog workload.
 *
 * Structure:
 *   name: ...
 *   drainBacklogConfig:
 *     backlogSizeGB: 1
 *     backlogProducerRate: 0
 *     ...
 *   baseWorkload:
 *     topics: 1
 *     partitionsPerTopic: 6
 *     ...
 *   scenarios:
 *     - name: partition-scaling
 *       partitionsPerTopic: [1, 6, 12]
 */
@JsonPropertyOrder({"baseKafkaConfig", "drainBacklogConfig", "baseWorkload", "scenarios"})
public class DrainBacklogWorkload {
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String name = "drain-backlog";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaConfigSummary baseKafkaConfig;

    public EngineConfig engineConfig = new EngineConfig();
    public DrainBacklogConfig drainBacklogConfig = new DrainBacklogConfig();
    public BaseWorkload baseWorkload = new BaseWorkload();
    public List<Scenario> scenarios = List.of();
}
