/*
 * Originally derived from io.openmessaging.benchmark.worker.commands.ConsumerAssignment
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark, Apache 2.0)
 * and substantially modified for Dimster. The current implementation is largely
 * Dimster-original; see NOTICE at the repo root for context.
 */
package dev.dimster.worker.commands;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.dimster.ConsumerType;

import java.util.List;

public record ConsumerAssignment(List<TopicConsumerGroup> topicConsumerGroups, ConsumerType consumerType,
                                 int consumerProcMsMin, int consumerProcMsMax) {

    @JsonCreator
    public ConsumerAssignment(
            @JsonProperty("topicConsumerGroups") List<TopicConsumerGroup> topicConsumerGroups,
            @JsonProperty("consumerType") ConsumerType consumerType,
            @JsonProperty("consumerProcMsMin") int consumerProcMsMin,
            @JsonProperty("consumerProcMsMax") int consumerProcMsMax) {
        this.topicConsumerGroups = topicConsumerGroups;
        this.consumerType = consumerType != null ? consumerType : ConsumerType.CONSUMER_GROUP;
        this.consumerProcMsMin = consumerProcMsMin;
        this.consumerProcMsMax = consumerProcMsMax;
    }

    public ConsumerAssignment(List<TopicConsumerGroup> topicConsumerGroups) {
        this(topicConsumerGroups, ConsumerType.CONSUMER_GROUP, 0, 0);
    }
}
