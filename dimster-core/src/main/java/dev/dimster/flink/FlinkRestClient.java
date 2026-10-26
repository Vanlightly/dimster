package dev.dimster.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal client for the Flink JobManager's REST API: just what the
 * coordinator needs to run one job per test point on the session cluster that
 * {@code dimster deploy} creates. It uploads the runner jar shipped in the
 * Dimster image, runs it, polls job state and cancels jobs. Plain HTTP and
 * Jackson, so the coordinator needs no Flink dependency.
 */
public class FlinkRestClient {

    private static final Logger log = LoggerFactory.getLogger(FlinkRestClient.class);

    /** The JobManager Service that {@code dimster deploy} creates in the namespace. */
    public static final String DEFAULT_URL = "http://flink-jobmanager:8081";
    /** Where the Dimster image ships the runner jar, relative to its working directory. */
    public static final String DEFAULT_JAR = "flink/dimster-flink-runner.jar";
    static final String ENTRY_CLASS = "dev.dimster.flink.runner.DimsterSqlJob";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final Path jarPath;
    private final HttpClient http;
    /** Id of the uploaded runner jar, uploaded on first submit. */
    private String jarId;

    public record Overview(int taskManagers, int slotsTotal, int slotsAvailable, String flinkVersion) {}

    public record JobSummary(String id, String name, String state) {}

    /** A job's state and the status of each of its vertices (operator chains). */
    public record JobDetails(String id, String name, String state, List<String> vertexStatuses) {
        public boolean allVerticesRunning() {
            return !vertexStatuses.isEmpty() && vertexStatuses.stream().allMatch("RUNNING"::equals);
        }
    }

    public FlinkRestClient(String baseUrl, Path jarPath) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.jarPath = jarPath;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** Configured by {@code DIMSTER_FLINK_REST_URL} and {@code DIMSTER_FLINK_JAR}, else the defaults. */
    public static FlinkRestClient fromEnvironment() {
        String url = System.getenv().getOrDefault("DIMSTER_FLINK_REST_URL", DEFAULT_URL);
        String jar = System.getenv().getOrDefault("DIMSTER_FLINK_JAR", DEFAULT_JAR);
        return new FlinkRestClient(url, Path.of(jar));
    }

    public String baseUrl() {
        return baseUrl;
    }

    public Overview overview() throws IOException {
        JsonNode n = get("/overview");
        return new Overview(n.path("taskmanagers").asInt(), n.path("slots-total").asInt(),
                n.path("slots-available").asInt(), n.path("flink-version").asText());
    }

    public List<JobSummary> listJobs() throws IOException {
        List<JobSummary> jobs = new ArrayList<>();
        for (JsonNode j : get("/jobs/overview").path("jobs")) {
            jobs.add(new JobSummary(j.path("jid").asText(), j.path("name").asText(), j.path("state").asText()));
        }
        return jobs;
    }

    public JobDetails job(String jobId) throws IOException {
        JsonNode n = get("/jobs/" + jobId);
        List<String> statuses = new ArrayList<>();
        for (JsonNode v : n.path("vertices")) {
            statuses.add(v.path("status").asText());
        }
        return new JobDetails(n.path("jid").asText(), n.path("name").asText(), n.path("state").asText(), statuses);
    }

    /** The job's restart count (the {@code numRestarts} job metric), or -1 when unavailable. */
    public long numRestarts(String jobId) throws IOException {
        for (JsonNode m : get("/jobs/" + jobId + "/metrics?get=numRestarts")) {
            if ("numRestarts".equals(m.path("id").asText())) {
                return m.path("value").asLong(-1);
            }
        }
        return -1;
    }

    /** The job's root exception (with stack trace) for error messages, or empty. */
    public String rootException(String jobId) {
        try {
            JsonNode n = get("/jobs/" + jobId + "/exceptions");
            return n.path("root-exception").asText("");
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * Runs the runner jar with one program argument and returns the job id.
     * Uploads the jar on first use, and again if the JobManager has lost it
     * (a JobManager restart clears uploaded jars).
     */
    public String submit(String programArg) throws IOException {
        if (jarId == null) {
            jarId = upload();
        }
        HttpResponse<String> response = runJar(programArg);
        if (response.statusCode() == 404) {
            log.info("Runner jar {} is no longer on the Flink cluster, uploading it again", jarId);
            jarId = upload();
            response = runJar(programArg);
        }
        JsonNode body = checkOk("POST /jars/" + jarId + "/run", response);
        String jobId = body.path("jobid").asText(null);
        if (jobId == null) {
            throw new IOException("Flink did not return a job id: " + response.body());
        }
        return jobId;
    }

    public void cancel(String jobId) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/jobs/" + jobId + "?mode=cancel"))
                .timeout(Duration.ofSeconds(30))
                .method("PATCH", HttpRequest.BodyPublishers.noBody())
                .build();
        checkOk("PATCH /jobs/" + jobId, send(request));
    }

    private HttpResponse<String> runJar(String programArg) throws IOException {
        String body = MAPPER.writeValueAsString(Map.of(
                "entryClass", ENTRY_CLASS,
                "programArgsList", List.of(programArg)));
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/jars/" + jarId + "/run"))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(request);
    }

    /** Uploads the runner jar as multipart form data and returns its jar id. */
    private String upload() throws IOException {
        if (!Files.isReadable(jarPath)) {
            throw new IOException("Flink runner jar not found at " + jarPath.toAbsolutePath());
        }
        String boundary = "dimster-" + UUID.randomUUID();
        var body = new ByteArrayOutputStream();
        body.write(("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jarPath.getFileName() + "\"\r\n"
                + "Content-Type: application/x-java-archive\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        body.write(Files.readAllBytes(jarPath));
        body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/jars/upload"))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();
        JsonNode n = checkOk("POST /jars/upload", send(request));
        // The jar id is the file name of the stored jar.
        String filename = n.path("filename").asText();
        String id = filename.substring(filename.lastIndexOf('/') + 1);
        log.info("Uploaded Flink runner jar {} to {} as {}", jarPath, baseUrl, id);
        return id;
    }

    private JsonNode get(String path) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        return checkOk("GET " + path, send(request));
    }

    private HttpResponse<String> send(HttpRequest request) throws IOException {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted calling Flink at " + request.uri(), e);
        }
    }

    private static JsonNode checkOk(String what, HttpResponse<String> response) throws IOException {
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Flink REST " + what + " failed with HTTP " + status + ": " + errors(response.body()));
        }
        String body = response.body();
        return body == null || body.isBlank() ? MAPPER.createObjectNode() : MAPPER.readTree(body);
    }

    /** Flink returns {@code {"errors": [...]}}; the entries can be long stack traces. */
    private static String errors(String body) {
        try {
            JsonNode errors = MAPPER.readTree(body).path("errors");
            if (errors.isArray() && !errors.isEmpty()) {
                List<String> parts = new ArrayList<>();
                errors.forEach(e -> parts.add(e.asText()));
                return String.join("\n", parts);
            }
        } catch (IOException ignored) {
            // Not JSON: fall through to the raw body.
        }
        return body;
    }
}
