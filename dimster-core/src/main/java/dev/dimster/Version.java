package dev.dimster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime-accessible Dimster version, exposed to anything that needs to
 * record "which Dimster produced this artifact" — primarily the result
 * JSON's metadata block, where it's the load-bearing field for
 * reproducibility once users start cutting git tags.
 *
 * The value is read once at class-load time from
 * {@code /dimster-version.properties} on the classpath. Maven resource
 * filtering (see dimster-core/pom.xml) substitutes {@code ${project.version}}
 * into that file at build time, so the runtime value always matches the
 * parent pom's declared version.
 *
 * If the properties file is missing (e.g. running tests from an IDE that
 * skips resource filtering, or a packaging glitch dropped the file from
 * the jar), the version falls back to {@value #UNKNOWN}. A WARN is logged
 * once at class load so the cause is visible without crashing the engine
 * — a missing version string is bad for reproducibility but not fatal to
 * running a benchmark.
 */
public final class Version {

    private static final Logger log = LoggerFactory.getLogger(Version.class);

    /** Sentinel returned when the version properties file cannot be loaded. */
    public static final String UNKNOWN = "unknown";

    private static final String RESOURCE_PATH = "/dimster-version.properties";
    private static final String VERSION;

    static {
        VERSION = loadVersion();
    }

    private Version() {
        // Utility class — never instantiated.
    }

    /**
     * Returns the Dimster version string from the bundled properties file
     * (e.g. {@code "0.1.0"} or {@code "0.1.0-SNAPSHOT"}), or
     * {@link #UNKNOWN} if the file is missing or unreadable.
     */
    public static String get() {
        return VERSION;
    }

    private static String loadVersion() {
        try (InputStream in = Version.class.getResourceAsStream(RESOURCE_PATH)) {
            if (in == null) {
                log.warn("Resource {} not found on classpath — Dimster version will be reported as '{}'. "
                        + "Result reproducibility will be degraded. This usually means the build skipped "
                        + "resource filtering or the resource was excluded from the jar.",
                        RESOURCE_PATH, UNKNOWN);
                return UNKNOWN;
            }
            Properties props = new Properties();
            props.load(in);
            String v = props.getProperty("version");
            if (v == null || v.isBlank() || v.equals("${project.version}")) {
                // The literal "${project.version}" reaching runtime means
                // resource filtering didn't run — the placeholder was
                // copied verbatim. Distinguish this from "missing file" so
                // an operator can pinpoint a build-config bug vs a
                // packaging bug.
                log.warn("Property 'version' in {} is blank or unfiltered (got '{}'). "
                        + "Dimster version will be reported as '{}'. Check that Maven resource "
                        + "filtering is enabled for dimster-core.",
                        RESOURCE_PATH, v, UNKNOWN);
                return UNKNOWN;
            }
            return v;
        } catch (IOException e) {
            log.warn("Failed to read {} — Dimster version will be reported as '{}'.",
                    RESOURCE_PATH, UNKNOWN, e);
            return UNKNOWN;
        }
    }
}
