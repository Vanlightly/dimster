package dev.dimster.worker.topology;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.kafka.common.TopicPartition;

/**
 * Defines the JSON boundary representation of Kafka {@link TopicPartition}s
 * embedded in topology messages.
 *
 * <p>{@code TopicPartition} deliberately exposes {@code topic()} and
 * {@code partition()} rather than JavaBean getters. Jackson 2.13 therefore
 * treats it as an empty bean unless every mapper is configured with a custom
 * module. Keeping the codec beside the topology wire model prevents the
 * coordinator HTTP client, worker HTTP server, and topology file recorder
 * from drifting onto different mapper configurations.
 */
public final class TopicPartitionJson {

    private TopicPartitionJson() {
    }

    /** Writes the stable object shape used in worker requests and topology files. */
    public static final class Serializer extends StdSerializer<TopicPartition> {

        public Serializer() {
            super(TopicPartition.class);
        }

        @Override
        public void serialize(TopicPartition value, JsonGenerator generator,
                              SerializerProvider provider) throws IOException {
            generator.writeStartObject();
            generator.writeStringField("topic", value.topic());
            generator.writeNumberField("partition", value.partition());
            generator.writeEndObject();
        }
    }

    /** Reconstructs Kafka's value object while rejecting malformed topology input. */
    public static final class Deserializer extends StdDeserializer<TopicPartition> {

        public Deserializer() {
            super(TopicPartition.class);
        }

        @Override
        public TopicPartition deserialize(JsonParser parser, DeserializationContext context)
                throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            if (!node.isObject()) {
                throw JsonMappingException.from(parser,
                        "TopicPartition must be an object with topic and partition fields");
            }

            JsonNode topicNode = node.get("topic");
            JsonNode partitionNode = node.get("partition");
            if (topicNode == null || !topicNode.isTextual() || topicNode.asText().isEmpty()) {
                throw JsonMappingException.from(parser,
                        "TopicPartition topic must be a non-empty string");
            }
            if (partitionNode == null || !partitionNode.isIntegralNumber()
                    || !partitionNode.canConvertToInt() || partitionNode.intValue() < 0) {
                throw JsonMappingException.from(parser,
                        "TopicPartition partition must be a non-negative integer");
            }

            return new TopicPartition(topicNode.asText(), partitionNode.intValue());
        }
    }
}
