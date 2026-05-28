package dev.dimster.worker.topology;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import dev.dimster.ConsumerType;
import dev.dimster.utils.distributor.MessageDistributorType;

/**
 * Smoke tests for {@link TopologyFormatter}: we don't pin down the exact
 * formatting (would be brittle), just check that the output mentions the
 * key facts that operators rely on when reading the printed topology.
 */
public class TopologyFormatterTest {

    @Test
    public void format_includesCaptionTopicsWorkersProducersAndConsumers() {
        var planner = newPlanner();
        var topology = planner.plan(new TopologyPlanner.TopologySpec(
                "p", 2, 4,
                /*producersPerTopic*/ 2,
                /*consumerGroupsPerTopic*/ 1,
                /*consumersPerGroup*/ 2,
                ConsumerType.CONSUMER_GROUP,
                0, 0, 1,
                MessageDistributorType.PINNED_PARTITIONS, 1.0,
                /*correctness*/ false,
                /*staticConsumerPartitions*/ false));

        String dump = TopologyFormatter.format("Test caption", topology);

        assertTrue("contains caption", dump.contains("Test caption"));
        assertTrue("mentions Topics header", dump.contains("Topics ("));
        assertTrue("mentions Workers header", dump.contains("Workers ("));
        assertTrue("lists at least one topic", dump.contains("p-s1-0000"));
        assertTrue("shows partition count", dump.contains("(4 partitions)"));
        assertTrue("shows producer entry with index", dump.contains("producer-p-s1-0000-i0"));
        assertTrue("shows pinned partitions", dump.contains("pinned=[0,1]") || dump.contains("pinned=[2,3]"));
        assertTrue("shows consumer entry", dump.contains("consumer-p-s1-0000-"));
        assertTrue("shows dynamic assignment for non-static", dump.contains("(dynamic)"));
    }

    @Test
    public void format_showsStaticPartitionsWhenSet() {
        var planner = newPlanner();
        var topology = planner.plan(new TopologyPlanner.TopologySpec(
                "p", 1, 4, 0, 1, 2, ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                MessageDistributorType.NO_KEY, 1.0, false, /*staticConsumerPartitions*/ true));

        String dump = TopologyFormatter.format(null, topology);
        // Each consumer gets [0,2] or [1,3] — the static representation must show the topic+partitions.
        assertTrue("static partitions appear", dump.contains("p-s1-0000[0,2]") || dump.contains("p-s1-0000[1,3]"));
    }

    @Test
    public void format_emptyCaptionOmitsLeadingNewline() {
        var topology = newPlanner().plan(new TopologyPlanner.TopologySpec(
                "p", 1, 1, 0, 0, 0, ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                MessageDistributorType.NO_KEY, 1.0, false, false));
        String dump = TopologyFormatter.format("", topology);
        assertTrue("starts with Topics line, not blank", dump.startsWith("  Topics"));
    }

    private static TopologyPlanner newPlanner() {
        AtomicInteger counter = new AtomicInteger(0);
        Supplier<String> suffix = () -> "s" + counter.incrementAndGet();
        return new TopologyPlanner(List.of("w1"),
                TopologyPlanner.WorkerMode.MIXED,
                new TopologyPlanner.RatioSplit(1, 1),
                suffix);
    }
}
