package dev.dimster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Checkpoint {

    private static final Logger log = LoggerFactory.getLogger(Checkpoint.class);
    private static final String CHECKPOINT_FILENAME = "checkpoint.json";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

    public String runRequestFile;
    public String resultsDir;
    public String mode;
    public java.util.List<String> workloadNames;
    public int completedWorkloadCount;
    public CurrentWorkload currentWorkload;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CurrentWorkload {
        public String name;
        public int nextTestPointIndex;
        public String partialResultFile;

        public CurrentWorkload() {}

        public CurrentWorkload(String name, int nextTestPointIndex, String partialResultFile) {
            this.name = name;
            this.nextTestPointIndex = nextTestPointIndex;
            this.partialResultFile = partialResultFile;
        }
    }

    public Checkpoint() {}

    public static Checkpoint load(File outputBase) {
        File checkpointFile = new File(outputBase, CHECKPOINT_FILENAME);
        if (!checkpointFile.exists()) {
            return null;
        }
        try {
            Checkpoint cp = mapper.readValue(checkpointFile, Checkpoint.class);
            log.info("Loaded checkpoint from {}: workload {}/{}, test point {}",
                    checkpointFile.getPath(),
                    cp.completedWorkloadCount, cp.workloadNames.size(),
                    cp.currentWorkload != null ? cp.currentWorkload.nextTestPointIndex : "n/a");
            return cp;
        } catch (IOException e) {
            log.error("Failed to read checkpoint file {}", checkpointFile.getPath(), e);
            return null;
        }
    }

    public void save(File outputBase) {
        File checkpointFile = new File(outputBase, CHECKPOINT_FILENAME);
        try {
            outputBase.mkdirs();
            writer.writeValue(checkpointFile, this);
        } catch (IOException e) {
            log.error("Failed to write checkpoint file {}", checkpointFile.getPath(), e);
        }
    }

    public static void delete(File outputBase) {
        File checkpointFile = new File(outputBase, CHECKPOINT_FILENAME);
        if (checkpointFile.exists()) {
            try {
                Files.delete(checkpointFile.toPath());
                log.info("Checkpoint deleted: {}", checkpointFile.getPath());
            } catch (IOException e) {
                log.warn("Failed to delete checkpoint file {}", checkpointFile.getPath(), e);
            }
        }
    }
}
