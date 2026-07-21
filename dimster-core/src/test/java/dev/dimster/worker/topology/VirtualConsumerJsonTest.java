package dev.dimster.worker.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.utils.distributor.MessageDistributorType;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the static-assignment topology contract shared by the coordinator
 * and remote workers. These tests use a plain {@link ObjectMapper}, matching
 * the independent mappers used by the HTTP client, HTTP server, and topology
 * recorder, so mapper-specific registration cannot hide a broken wire model.
 */
public class VirtualConsumerJsonTest {

    // Match WorkerHttpServer: ConsumerSet's convenience isEmpty() method is
    // emitted as a derived property and intentionally ignored on input.
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void consumerSetRoundTripsStaticPartitions() throws Exception {
        ConsumerSet expected = consumerSet(List.of(
                new TopicPartition("topic-a", 0),
                new TopicPartition("topic-a", 3)));

        String json = mapper.writeValueAsString(expected);
        JsonNode tree = mapper.readTree(json);
        JsonNode firstPartition = tree.path("consumers").get(0)
                .path("staticPartitions").get(0);

        assertEquals("topic-a", firstPartition.path("topic").asText());
        assertEquals(0, firstPartition.path("partition").asInt());
        assertEquals(expected, mapper.readValue(json, ConsumerSet.class));
    }

    @Test
    public void consumerSetRoundTripsDynamicAssignment() throws Exception {
        ConsumerSet expected = consumerSet(List.of());

        String json = mapper.writeValueAsString(expected);

        assertEquals(expected, mapper.readValue(json, ConsumerSet.class));
    }

    @Test
    public void malformedPartitionIsRejected() throws Exception {
        String json = mapper.writeValueAsString(consumerSet(List.of()));
        String malformed = json.replace("\"staticPartitions\":[]",
                "\"staticPartitions\":[{\"topic\":\"topic-a\",\"partition\":-1}]");

        assertThrows(JsonMappingException.class,
                () -> mapper.readValue(malformed, ConsumerSet.class));
    }

    @Test
    public void topologyRecorderWritesStaticPartitions() throws Exception {
        VirtualConsumer consumer = new VirtualConsumer(
                "http://worker-0:8080", "topic-a", "group-a", 0,
                List.of(new TopicPartition("topic-a", 2)));
        VirtualTopology topology = new VirtualTopology(
                Map.of("topic-a", 4), List.of(), List.of(consumer),
                ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                ConsumerProcMode.BLOCKING, 1, MessageDistributorType.NO_KEY,
                1.0, 0);
        File output = temporaryFolder.newFile("topology.json");

        new JsonFileTopologyRecorder("static-assignment", output)
                .record("default", topology);

        JsonNode partition = mapper.readTree(output)
                .path("testPoints").get(0)
                .path("topology").path("consumers").get(0)
                .path("staticPartitions").get(0);
        assertEquals("topic-a", partition.path("topic").asText());
        assertEquals(2, partition.path("partition").asInt());
    }

    private ConsumerSet consumerSet(List<TopicPartition> staticPartitions) {
        VirtualConsumer consumer = new VirtualConsumer(
                "http://worker-0:8080", "topic-a", "group-a", 0, staticPartitions);
        return new ConsumerSet(List.of(consumer), ConsumerType.CONSUMER_GROUP,
                0, 0, 1, ConsumerProcMode.BLOCKING, 1);
    }
}
