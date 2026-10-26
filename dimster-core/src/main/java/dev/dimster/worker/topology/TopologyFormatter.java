package dev.dimster.worker.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * Renders a {@link VirtualTopology} as a multi-line text dump for log output.
 *
 * <p>Grouped by worker so the reader can quickly see what each worker will
 * host. Producers and consumers within a worker are sorted by topic then
 * index for stable, diff-friendly output.
 *
 * <p>Used by {@code BaseEngine} when {@code engineConfig.printTopology = true};
 * always called via the engine's topology recorder regardless. Kept as a pure
 * function on {@link VirtualTopology} so tests can compare expected vs actual
 * dumps without going through the engine layer.
 */
public final class TopologyFormatter {

    private TopologyFormatter() {}

    /** Returns a multi-line text dump of the topology, prefixed by an optional caption. */
    public static String format(String caption, VirtualTopology topology) {
        var sb = new StringBuilder();
        if (caption != null && !caption.isEmpty()) {
            sb.append(caption).append('\n');
        }

        // Topic / partition summary first — the cluster-side facts everything else was built from.
        sb.append("  Topics (").append(topology.topicPartitionCounts().size()).append("):\n");
        for (var e : topology.topicPartitionCounts().entrySet()) {
            sb.append("    - ").append(e.getKey())
              .append(" (").append(e.getValue()).append(" partitions)\n");
        }
        if (!topology.pipelineTopics().isEmpty()) {
            sb.append("  Flink pipelines (").append(topology.pipelineTopics().size()).append("):\n");
            for (var e : topology.pipelineTopics().entrySet()) {
                sb.append("    - ").append(e.getKey()).append(" -> ").append(e.getValue()).append('\n');
            }
        }

        // Workers, in topology order. For each worker show producers then consumers.
        sb.append("  Workers (").append(topology.workers().size())
          .append(", activeProducerWorkers=").append(topology.activeProducerWorkerCount()).append("):\n");

        for (String worker : topology.workers()) {
            sb.append("    ").append(worker).append(":\n");

            List<VirtualProducer> producers = new ArrayList<>(topology.producersForWorker(worker));
            producers.sort(Comparator.comparing(VirtualProducer::topic)
                    .thenComparingInt(VirtualProducer::producerIdx));
            if (producers.isEmpty()) {
                sb.append("      producers: (none)\n");
            } else {
                sb.append("      producers (").append(producers.size()).append("):\n");
                for (VirtualProducer vp : producers) {
                    sb.append("        - ").append(vp.name())
                      .append(" topic=").append(vp.topic())
                      .append(" pinned=").append(formatPartitions(vp.pinnedPartitions()))
                      .append('\n');
                }
            }

            // Group consumers by (topic, group) for compact display.
            List<VirtualConsumer> consumers = new ArrayList<>(topology.consumersForWorker(worker));
            consumers.sort(Comparator.comparing(VirtualConsumer::topic)
                    .thenComparing(VirtualConsumer::consumerGroup)
                    .thenComparingInt(VirtualConsumer::consumerIdx));
            if (consumers.isEmpty()) {
                sb.append("      consumers: (none)\n");
            } else {
                sb.append("      consumers (").append(consumers.size()).append("):\n");
                for (VirtualConsumer vc : consumers) {
                    sb.append("        - ").append(vc.name())
                      .append(" topic=").append(vc.topic())
                      .append(" group=").append(vc.consumerGroup())
                      .append(" partitions=").append(formatStaticPartitions(vc.staticPartitions()))
                      .append('\n');
                }
            }
        }

        return sb.toString();
    }

    /** {@code "[]"} for empty pinning, {@code "[0,1,2]"} otherwise. */
    private static String formatPartitions(int[] pinned) {
        if (pinned.length == 0) return "[]";
        return Arrays.toString(pinned).replace(" ", "");
    }

    /**
     * For each topic in the static-partition list, show a compact {@code topic[a,b,c]}
     * form. Empty list renders as {@code (dynamic)} to make it obvious dynamic group
     * assignment is in use.
     */
    private static String formatStaticPartitions(List<TopicPartition> staticPartitions) {
        if (staticPartitions.isEmpty()) return "(dynamic)";
        Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
        for (TopicPartition tp : staticPartitions) {
            byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
        }
        var sb = new StringBuilder();
        boolean first = true;
        for (var e : byTopic.entrySet()) {
            if (!first) sb.append(',');
            sb.append(e.getKey()).append(e.getValue().toString().replace(" ", ""));
            first = false;
        }
        return sb.toString();
    }
}
