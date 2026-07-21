package dev.dimster.worker;

import static org.junit.Assert.assertEquals;

import dev.dimster.engines.correctness.ModelChecker;

import org.junit.Test;

/**
 * Locks down conversion of live checker violations into correctness result
 * events. The comparison timeline depends on detection timestamps surviving
 * result collection unchanged.
 */
public class LocalWorkerCorrectnessResultTest {

    @Test
    public void violationTimestampUsesDetectionTimeForEveryViolationType() {
        assertEquals(101L, LocalWorker.violationTimestampMs(
                new ModelChecker.DuplicateViolation(1, 2, "topic", 3, 4, 101L)));
        assertEquals(202L, LocalWorker.violationTimestampMs(
                new ModelChecker.OutOfOrderViolation(1, 2, 4, "topic", 3, 202L)));
        assertEquals(303L, LocalWorker.violationTimestampMs(
                new ModelChecker.CrcFailureViolation(1, 2, "topic", 3, 303L)));
    }
}
