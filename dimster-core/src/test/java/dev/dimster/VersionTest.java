package dev.dimster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Locks in the contract between Maven's resource filtering and Java's
 * runtime version lookup. If any of these break, every result file ships
 * with a bogus version and reproducibility silently rots — so the
 * assertions here are deliberately tight rather than just "not null".
 */
public class VersionTest {

    @Test
    public void version_is_populated_from_filtered_resource() {
        String v = Version.get();
        assertNotNull("Version.get() must never return null", v);
        assertFalse("Version.get() must not be empty", v.isEmpty());
        // The literal placeholder reaching runtime means Maven resource
        // filtering didn't fire — the properties file was copied verbatim
        // into the jar. This is the specific bug we're guarding against.
        assertNotEquals("Version.get() must be the filtered value, not the placeholder",
                "${project.version}", v);
        // Likewise, the UNKNOWN sentinel reaching us under a normal `mvnw
        // test` invocation means the resource file is missing from the
        // test classpath — also a build-config bug, not a runtime
        // condition that should occur during CI.
        assertNotEquals("Version.get() must be the real version, not the UNKNOWN sentinel. "
                        + "Check that dimster-core/src/main/resources is included on the test classpath "
                        + "and that resource filtering is enabled.",
                Version.UNKNOWN, v);
    }

    @Test
    public void version_looks_like_semver() {
        // Either "0.X.Y" or "0.X.Y-SUFFIX" (SNAPSHOT, rc.1, etc.). The
        // bump-version script enforces the same shape, so any version
        // that doesn't match here came from a hand-edit that bypassed
        // the script — worth surfacing as a test failure rather than
        // letting it land in a result JSON.
        String v = Version.get();
        assertTrue("Version '" + v + "' does not look like SemVer (expected 0.X.Y or 0.X.Y-SUFFIX)",
                v.matches("^\\d+\\.\\d+\\.\\d+(-[A-Za-z0-9.]+)?$"));
    }

    @Test
    public void result_metadata_constructor_populates_dimster_version() {
        // The one-place-to-populate guarantee: any caller doing
        // `new ResultMetadata()` cannot accidentally ship a result with
        // no version string. If this breaks, every new result type added
        // in the future is at risk of silently omitting the field.
        ResultMetadata m = new ResultMetadata();
        assertEquals("ResultMetadata constructor must mirror Version.get()",
                Version.get(), m.dimsterVersion);
    }
}
