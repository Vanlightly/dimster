package dev.dimster.worker.topology;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TopologyRecorder} that buffers per-test-point topologies in memory and
 * rewrites a single JSON file after every {@link #record} call. Used by the
 * harness so the topology file stays current even if a run is interrupted
 * mid-way through its test points.
 *
 * <p>The output file shape is:
 *
 * <pre>{@code
 * {
 *   "workloadName": "...",
 *   "writtenAt": "2026-05-07T11:00:00Z",
 *   "testPoints": [
 *     { "label": "...", "topology": { ... } },
 *     ...
 *   ]
 * }
 * }</pre>
 */
public final class JsonFileTopologyRecorder implements TopologyRecorder {

    private static final Logger log = LoggerFactory.getLogger(JsonFileTopologyRecorder.class);

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private final String workloadName;
    private final File outputFile;
    private final List<Entry> entries = new ArrayList<>();

    public JsonFileTopologyRecorder(String workloadName, File outputFile) {
        this.workloadName = workloadName;
        this.outputFile = outputFile;
    }

    @Override
    public synchronized void record(String testPointLabel, VirtualTopology topology) {
        entries.add(new Entry(testPointLabel, topology));
        try {
            writer.writeValue(outputFile, snapshot());
        } catch (IOException e) {
            log.warn("Failed to write topology file {}: {}", outputFile.getPath(), e.toString());
        }
    }

    private Snapshot snapshot() {
        return new Snapshot(workloadName, Instant.now().toString(), List.copyOf(entries));
    }

    /** Serialized payload — one record per workload-run output file. */
    public record Snapshot(String workloadName, String writtenAt, List<Entry> testPoints) {}

    public record Entry(String label, VirtualTopology topology) {}
}
