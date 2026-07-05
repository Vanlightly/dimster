package dev.dimster.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.ClientStatus;
import org.junit.Test;

/**
 * Tests the compact health DTO used on the worker/coordinator boundary. The
 * DTO is intentionally responsible for status counting and failure-detail
 * capping so the rest of the codebase does not duplicate health semantics.
 */
public class ClientHealthTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void emptyHealthHasZeroCounts() {
        ClientHealth health = ClientHealth.empty();

        assertTrue(health.isEmpty());
        assertEquals(0, health.producers().total());
        assertEquals(0, health.consumers().total());
        assertTrue(health.failedClients().isEmpty());
        assertFalse(health.hasFailures());
    }

    @Test
    public void countsRunningClientsWithoutDetails() {
        ClientHealth health = ClientHealth.builder()
                .addProducer(ClientStatus.running("producer-0", "PRODUCER"))
                .addConsumer(ClientStatus.running("consumer-0", "CLASSIC_CONSUMER"))
                .addConsumer(ClientStatus.running("consumer-1", "SHARE_CONSUMER"))
                .build();

        assertEquals(1, health.producers().total());
        assertEquals(1, health.producers().running());
        assertEquals(2, health.consumers().total());
        assertEquals(2, health.consumers().running());
        assertTrue(health.failedClients().isEmpty());
        assertTrue(health.allRunning());
    }

    @Test
    public void countsPendingAndRecreatingWithoutFailureDetails() {
        ClientHealth health = ClientHealth.builder()
                .addProducer(ClientStatus.pending("producer-0", "PRODUCER"))
                .addConsumer(ClientStatus.recreating("consumer-0", "CLASSIC_CONSUMER", "rebalance"))
                .build();

        assertEquals(1, health.producers().pending());
        assertEquals(1, health.consumers().recreating());
        assertTrue(health.failedClients().isEmpty());
        assertFalse(health.hasFailures());
        assertFalse(health.allRunning());
    }

    @Test
    public void includesFailedClientDetails() {
        ClientHealth health = ClientHealth.builder()
                .addProducer(ClientStatus.failed("producer-0", "PRODUCER", "send failed"))
                .build();

        assertEquals(1, health.producers().failed());
        assertTrue(health.hasFailures());
        assertEquals("producer-0", health.failedClients().get(0).name());
        assertEquals("send failed", health.failedClients().get(0).error());
    }

    @Test
    public void capsFailedClientDetails() {
        ClientHealth.Builder builder = ClientHealth.builder(2);
        builder.addProducer(ClientStatus.failed("producer-0", "PRODUCER", "first"));
        builder.addProducer(ClientStatus.failed("producer-1", "PRODUCER", "second"));
        builder.addProducer(ClientStatus.failed("producer-2", "PRODUCER", "third"));

        ClientHealth health = builder.build();

        assertEquals(3, health.producers().failed());
        assertEquals(2, health.failedClients().size());
        assertEquals(1, health.omittedFailedClients());
    }

    @Test
    public void defaultFailedClientLimitIsOneHundred() {
        ClientHealth.Builder builder = ClientHealth.builder();
        for (int i = 0; i < 101; i++) {
            builder.addProducer(ClientStatus.failed("producer-" + i, "PRODUCER", "failed"));
        }

        ClientHealth health = builder.build();

        assertEquals(100, health.failedClientLimit());
        assertEquals(101, health.producers().failed());
        assertEquals(100, health.failedClients().size());
        assertEquals(1, health.omittedFailedClients());
    }

    @Test
    public void aggregatesCountsAndRecapsFailureDetails() {
        List<ClientHealth> workerHealth = new ArrayList<>();
        workerHealth.add(ClientHealth.builder(2)
                .addProducer(ClientStatus.running("producer-0", "PRODUCER"))
                .addConsumer(ClientStatus.failed("consumer-0", "CLASSIC_CONSUMER", "poll failed"))
                .build());
        workerHealth.add(ClientHealth.builder(2)
                .addProducer(ClientStatus.failed("producer-1", "PRODUCER", "send failed"))
                .addProducer(ClientStatus.failed("producer-2", "PRODUCER", "timeout"))
                .build());

        ClientHealth aggregate = ClientHealth.aggregate(workerHealth);

        assertEquals(3, aggregate.producers().total());
        assertEquals(1, aggregate.producers().running());
        assertEquals(2, aggregate.producers().failed());
        assertEquals(1, aggregate.consumers().failed());
        assertEquals(3, aggregate.failedClients().size());
        assertEquals(0, aggregate.omittedFailedClients());
    }

    @Test
    public void jsonRoundTripPreservesCompactShape() throws Exception {
        ClientHealth original = ClientHealth.builder()
                .addProducer(ClientStatus.running("producer-0", "PRODUCER"))
                .addConsumer(ClientStatus.failed("consumer-0", "SHARE_CONSUMER", "poll failed"))
                .build();

        String json = mapper.writeValueAsString(original);
        ClientHealth deserialized = mapper.readValue(json, ClientHealth.class);

        assertEquals(original, deserialized);
    }
}
