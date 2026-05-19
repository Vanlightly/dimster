package dev.dimster.worker;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.worker.commands.ClientStatus;
import org.junit.Test;

import java.util.List;

public class ClientStatusTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void pendingFactoryHasNullError() {
        ClientStatus cs = ClientStatus.pending("consumer-test", "CLASSIC_CONSUMER");
        assertEquals("consumer-test", cs.name());
        assertEquals("CLASSIC_CONSUMER", cs.clientType());
        assertEquals("PENDING", cs.status());
        assertNull(cs.error());
    }

    @Test
    public void runningFactoryHasNullError() {
        ClientStatus cs = ClientStatus.running("producer-test", "PRODUCER");
        assertEquals("producer-test", cs.name());
        assertEquals("PRODUCER", cs.clientType());
        assertEquals("RUNNING", cs.status());
        assertNull(cs.error());
    }

    @Test
    public void failedFactoryHasError() {
        ClientStatus cs = ClientStatus.failed("consumer-test", "CLASSIC_CONSUMER", "some error");
        assertEquals("consumer-test", cs.name());
        assertEquals("CLASSIC_CONSUMER", cs.clientType());
        assertEquals("FAILED", cs.status());
        assertEquals("some error", cs.error());
    }

    @Test
    public void jsonRoundTrip() throws Exception {
        List<ClientStatus> original = List.of(
                ClientStatus.pending("kafka-consumer-t1", "CLASSIC_CONSUMER"),
                ClientStatus.running("producer-t1", "PRODUCER"),
                ClientStatus.failed("kafka-consumer-t2", "SHARE_CONSUMER", "connection refused"));

        String json = mapper.writeValueAsString(original);
        List<ClientStatus> deserialized = mapper.readValue(json, new TypeReference<>() {});

        assertEquals(original, deserialized);
    }
}
