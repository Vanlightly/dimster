package dev.dimster.flink.runner;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Everything one Flink job needs, as built by the coordinator
 * ({@code dev.dimster.flink.FlinkJobSpecBuilder} in dimster-core) and passed as
 * the single, base64-encoded program argument of the runner jar.
 *
 * <p>The two modules do not share this class (they target different Java
 * releases and the runner is shaded), so the JSON field names are the
 * contract. Unknown fields fail decoding so a mismatch is loud.
 *
 * @param jobName              Flink job name; {@code dimster-} prefixed so the
 *                             coordinator can find and cancel leftovers
 * @param sql                  the workload's SELECT over {@code source}
 * @param format               {@code AVRO} or {@code JSON}
 * @param columns              the dataset's columns after {@code _dimster}, in
 *                             schema order, with Flink types including nullability
 * @param pipelines            one (input topic, output topic) pair per test topic
 * @param sourceGroupId        Kafka group id of the Flink source
 * @param kafkaProperties      Kafka client properties for both tables
 *                             (bootstrap and security), without the {@code properties.} prefix
 * @param parallelism          job parallelism
 * @param deliveryGuarantee    {@code AT_LEAST_ONCE}, {@code EXACTLY_ONCE} or {@code NONE}
 * @param checkpointIntervalMs checkpoint interval; 0 disables checkpointing
 * @param config               extra Flink job configuration
 * @param sourceOptions        extra Kafka source table options
 * @param sinkOptions          extra Kafka sink table options
 * @param outputColumns        {@code ALL} or {@code DIMSTER_ONLY} (the sink
 *                             drops the query's data columns); null means {@code ALL}
 */
public record FlinkJobSpec(
        String jobName,
        String sql,
        String format,
        List<Column> columns,
        List<Pipeline> pipelines,
        String sourceGroupId,
        Map<String, String> kafkaProperties,
        int parallelism,
        String deliveryGuarantee,
        long checkpointIntervalMs,
        Map<String, String> config,
        Map<String, String> sourceOptions,
        Map<String, String> sinkOptions,
        String outputColumns) {

    private static final String ALL = "ALL";
    private static final String DIMSTER_ONLY = "DIMSTER_ONLY";

    /** A source column: name and Flink SQL type, e.g. {@code BIGINT NOT NULL}. */
    public record Column(String name, String type) {}

    /** One input topic and the output topic its query results go to. */
    public record Pipeline(String sourceTopic, String sinkTopic) {}

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    public FlinkJobSpec {
        columns = columns == null ? List.of() : List.copyOf(columns);
        pipelines = pipelines == null ? List.of() : List.copyOf(pipelines);
        kafkaProperties = kafkaProperties == null ? Map.of() : Map.copyOf(kafkaProperties);
        config = config == null ? Map.of() : Map.copyOf(config);
        sourceOptions = sourceOptions == null ? Map.of() : Map.copyOf(sourceOptions);
        sinkOptions = sinkOptions == null ? Map.of() : Map.copyOf(sinkOptions);
        outputColumns = outputColumns == null ? ALL : outputColumns;
        if (!outputColumns.equals(ALL) && !outputColumns.equals(DIMSTER_ONLY)) {
            throw new IllegalArgumentException("Unknown outputColumns: " + outputColumns);
        }
    }

    /** Whether the sink writes only Dimster's fields, not the query's data columns. */
    public boolean dimsterOnly() {
        return DIMSTER_ONLY.equals(outputColumns);
    }

    public static FlinkJobSpec fromJson(String json) throws IOException {
        return MAPPER.readValue(json, FlinkJobSpec.class);
    }

    public static FlinkJobSpec fromBase64(String arg) throws IOException {
        return fromJson(new String(Base64.getDecoder().decode(arg), StandardCharsets.UTF_8));
    }
}
