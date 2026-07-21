package dev.dimster.engines.correctness;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.Worker;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;
import dev.dimster.worker.topology.VirtualTopology;

/**
 * Verifies correctness-mode restart policy independently from long-running
 * Kafka phases. The controller boundary is intentionally small so lifecycle,
 * validation, and distributed failure semantics can be tested synchronously.
 */
public class CorrectnessConsumerRestartControllerTest {

    @Test
    public void acceptsOnlyConsumerRestartDuringOpenWindow() {
        RecordingWorker worker = new RecordingWorker();
        CorrectnessConsumerRestartController controller = controller(worker, dynamicTopology(), 7);

        assertFalse(controller.applyModification(restartConsumers(2)).accepted());
        controller.openWindow();

        assertFalse(controller.applyModification(emptyRequest()).accepted());
        assertFalse(controller.applyModification(restartProducers(1)).accepted());
        assertFalse(controller.applyModification(restartConsumersWithOverride(1)).accepted());
        assertFalse(controller.applyModification(restartConsumers(-1)).accepted());

        ModifyResult result = controller.applyModification(restartConsumers(2));
        assertTrue(result.accepted());
        assertNotNull(worker.lastRestartedConsumers);
        assertEquals(2, worker.lastRestartedConsumers.consumers().size());

        controller.closeWindow();
        worker.lastRestartedConsumers = null;
        assertFalse(controller.applyModification(restartConsumers(1)).accepted());
        assertNull(worker.lastRestartedConsumers);
    }

    @Test
    public void resettingTestPointSeedRepeatsConsumerOrdinalSelection() {
        RecordingWorker worker = new RecordingWorker();
        CorrectnessConsumerRestartController controller = controller(worker, dynamicTopology(), 42);
        controller.openWindow();
        assertTrue(controller.applyModification(restartConsumers(3)).accepted());
        List<Integer> first = selectedOrdinals(worker.lastRestartedConsumers);

        controller.prepareTest(dynamicTopology(), 42);
        controller.openWindow();
        assertTrue(controller.applyModification(restartConsumers(3)).accepted());
        List<Integer> repeated = selectedOrdinals(worker.lastRestartedConsumers);

        assertEquals(first, repeated);
        assertEquals(3, first.stream().distinct().count());
    }

    @Test
    public void restartPreservesTopologyConsumerSettingsAndCapsToAll() {
        RecordingWorker worker = new RecordingWorker();
        VirtualTopology topology = dynamicTopology();
        CorrectnessConsumerRestartController controller = controller(worker, topology, 0);
        controller.openWindow();

        assertTrue(controller.applyModification(restartConsumers(100)).accepted());
        ConsumerSet restarted = worker.lastRestartedConsumers;
        assertEquals(topology.consumers(), restarted.consumers());
        assertEquals(topology.consumerType(), restarted.consumerType());
        assertEquals(topology.consumerProcMsMin(), restarted.consumerProcMsMin());
        assertEquals(topology.consumerProcMsMax(), restarted.consumerProcMsMax());
        assertEquals(topology.consumerProcParallelism(), restarted.consumerProcParallelism());
        assertEquals(topology.consumerProcMode(), restarted.consumerProcMode());
        assertEquals(topology.consumerProcCapacity(), restarted.consumerProcCapacity());
    }

    @Test
    public void manualPartitionAssignmentIsRejectedWithoutDispatch() {
        RecordingWorker worker = new RecordingWorker();
        CorrectnessConsumerRestartController controller = controller(worker, staticTopology(), 0);
        controller.openWindow();

        ModifyResult result = controller.applyModification(restartConsumers(1));

        assertFalse(result.accepted());
        assertTrue(result.message().contains("staticConsumerAssignment=false"));
        assertNull(worker.lastRestartedConsumers);
        assertTrue(controller.events().isEmpty());
    }

    @Test
    public void zeroIsAnAuditedNoOp() {
        RecordingWorker worker = new RecordingWorker();
        CorrectnessConsumerRestartController controller = controller(worker, dynamicTopology(), 0);
        controller.openWindow();

        ModifyResult result = controller.applyModification(restartConsumers(0));

        assertTrue(result.accepted());
        assertNull(worker.lastRestartedConsumers);
        assertEquals(1, controller.events().size());
        assertTrue(controller.events().getFirst().succeeded);
        assertTrue(controller.events().getFirst().selectedConsumerNames.isEmpty());
    }

    @Test
    public void workerFailureIsAuditedAndInvalidatesTest() {
        RecordingWorker worker = new RecordingWorker();
        worker.restartFailure = new IOException("worker unavailable");
        CorrectnessConsumerRestartController controller = controller(worker, dynamicTopology(), 0);
        controller.openWindow();

        ModifyResult result = controller.applyModification(restartConsumers(2));

        assertFalse(result.accepted());
        assertTrue(controller.restartDispatchFailed());
        assertEquals(1, controller.events().size());
        CorrectnessResult.ConsumerRestartEvent event = controller.events().getFirst();
        assertFalse(event.succeeded);
        assertEquals(2, event.selectedConsumerNames.size());
        assertTrue(event.message.contains("worker unavailable"));

        CorrectnessResult.TestOutcome outcome = new CorrectnessResult.TestOutcome();
        outcome.correctness.passed = true;
        controller.populateOutcome(outcome);
        assertFalse(outcome.correctness.passed);
        assertEquals(controller.events(), outcome.consumerRestarts);
    }

    @Test
    public void correctnessResultSerializesRestartAuditFields() throws Exception {
        CorrectnessResult.TestOutcome outcome = new CorrectnessResult.TestOutcome();
        outcome.consumerRestarts.add(new CorrectnessResult.ConsumerRestartEvent(
                "2026-07-21T08:00:00Z", 2, List.of("consumer-a", "consumer-b"),
                true, "restarted 2 consumers"));

        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsBytes(outcome));

        assertEquals(2, json.at("/consumerRestarts/0/requestedCount").asInt());
        assertEquals("consumer-a",
                json.at("/consumerRestarts/0/selectedConsumerNames/0").asText());
        assertTrue(json.at("/consumerRestarts/0/succeeded").asBoolean());
    }

    private CorrectnessConsumerRestartController controller(RecordingWorker worker,
                                                            VirtualTopology topology,
                                                            long seed) {
        CorrectnessConsumerRestartController controller =
                new CorrectnessConsumerRestartController(worker);
        controller.prepareTest(topology, seed);
        return controller;
    }

    private List<Integer> selectedOrdinals(ConsumerSet consumers) {
        return consumers.consumers().stream().map(VirtualConsumer::consumerIdx).toList();
    }

    private ModifyRequest emptyRequest() {
        return new ModifyRequest(null, null, null, null, null, null, null, null);
    }

    private ModifyRequest restartConsumers(int count) {
        return new ModifyRequest(null, null, null, null, count, null, null, null);
    }

    private ModifyRequest restartProducers(int count) {
        return new ModifyRequest(null, null, null, null, null, count, null, null);
    }

    private ModifyRequest restartConsumersWithOverride(int count) {
        return new ModifyRequest(null, null, null, null, count, null,
                Map.of("producerRate", 1000), null);
    }

    private VirtualTopology dynamicTopology() {
        return topology(false);
    }

    private VirtualTopology staticTopology() {
        return topology(true);
    }

    private VirtualTopology topology(boolean staticAssignment) {
        Map<String, Integer> topics = new LinkedHashMap<>();
        topics.put("topic-a", 5);
        List<VirtualConsumer> consumers = java.util.stream.IntStream.range(0, 5)
                .mapToObj(index -> new VirtualConsumer("worker-" + (index % 2), "topic-a",
                        "group-a", index,
                        staticAssignment ? List.of(new TopicPartition("topic-a", index)) : List.of()))
                .toList();
        return new VirtualTopology(topics, List.of(), consumers,
                ConsumerType.CONSUMER_GROUP, 3, 9, 4,
                ConsumerProcMode.CONTINUOUS, 321,
                MessageDistributorType.NO_KEY, 1.0, 0);
    }

    /** Worker stub that captures only the restart action under test. */
    private static class RecordingWorker implements Worker {
        ConsumerSet lastRestartedConsumers;
        IOException restartFailure;

        @Override
        public void restartConsumers(ConsumerSet consumers) throws IOException {
            if (restartFailure != null) throw restartFailure;
            lastRestartedConsumers = consumers;
        }

        @Override public void setKafkaConfig(File configurationFile) {}
        @Override public void setKafkaConfig(KafkaConfig config) {}
        @Override public void createProducers(List<VirtualProducer> producers) {}
        @Override public void createConsumers(ConsumerSet consumers) {}
        @Override public void probeProducers() {}
        @Override public void startLoad(ProducerWorkAssignment producerWorkAssignment) {}
        @Override public void adjustPublishRate(double publishRate) {}
        @Override public void pauseProducers() {}
        @Override public void resumeProducers() {}
        @Override public CountersStats getCountersStats() { return new CountersStats(0, 0); }
        @Override public long getProbesReceived() { return 0; }
        @Override public PeriodStats getPeriodStats() { return new PeriodStats(); }
        @Override public void resetStats() {}
        @Override public void stopAll() {}
        @Override public boolean isStopComplete() { return true; }
        @Override public void awaitStopComplete(Duration timeout) {}
        @Override public ClientHealth getHealth() { return ClientHealth.empty(); }
        @Override public void close() {}
    }
}
