package dev.dimster.kafka;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PartitionOffsetTracker}.
 *
 * <p>PartitionOffsetTracker handles offset gap tracking for CONTINUOUS mode
 * where records complete out-of-order. Kafka naturally has offset gaps due to
 * transaction markers, aborted transactions, and compaction.
 */
public class PartitionOffsetTrackerTest {

    @Test
    public void testNormalProcessingWithKafkaGaps() {
        // Scenario: Kafka offset gaps (transaction markers at 11, 14)
        // received: {10, 12, 13, 15}
        // completed: {10, 12, 15} (13 still inflight)
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(10);
        tracker.markReceived(12);
        tracker.markReceived(13);
        tracker.markReceived(15);

        tracker.markComplete(10);
        assertEquals("After completing 10", 10, tracker.getHighestCommittable());

        tracker.markComplete(12);
        assertEquals("After completing 12", 12, tracker.getHighestCommittable());

        tracker.markComplete(15);
        assertEquals("After completing 15 (13 still missing)", 12, tracker.getHighestCommittable());

        tracker.markComplete(13);
        assertEquals("After completing 13", 15, tracker.getHighestCommittable());
    }

    @Test
    public void testOutOfOrderCompletion() {
        // Scenario: Records complete in reverse order
        // received: {0, 1, 2}
        // completed in order: {2, 1, 0}
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);

        tracker.markComplete(2);
        assertEquals("After completing 2 (0,1 not complete)", -1, tracker.getHighestCommittable());

        tracker.markComplete(1);
        assertEquals("After completing 1 (0 not complete)", -1, tracker.getHighestCommittable());

        tracker.markComplete(0);
        assertEquals("After completing 0 (all complete)", 2, tracker.getHighestCommittable());
    }

    @Test
    public void testSequentialCompletion() {
        // Scenario: Records complete in order
        // received: {0, 1, 2, 3}
        // completed in order: {0, 1, 2, 3}
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);
        tracker.markReceived(3);

        tracker.markComplete(0);
        assertEquals("After completing 0", 0, tracker.getHighestCommittable());

        tracker.markComplete(1);
        assertEquals("After completing 1", 1, tracker.getHighestCommittable());

        tracker.markComplete(2);
        assertEquals("After completing 2", 2, tracker.getHighestCommittable());

        tracker.markComplete(3);
        assertEquals("After completing 3", 3, tracker.getHighestCommittable());
    }

    @Test
    public void testLargeGap() {
        // Scenario: Large offset gap (compaction removed 1-99)
        // received: {0, 100}
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(100);

        tracker.markComplete(0);
        assertEquals("After completing 0", 0, tracker.getHighestCommittable());

        tracker.markComplete(100);
        assertEquals("After completing 100", 100, tracker.getHighestCommittable());
    }

    @Test
    public void testFirstOffsetIsHole() {
        // Scenario: First received offset is not completed
        // received: {10, 12}
        // completed: {12}
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(10);
        tracker.markReceived(12);

        tracker.markComplete(12);
        assertEquals("After completing 12 (10 is a hole)", -1, tracker.getHighestCommittable());

        tracker.markComplete(10);
        assertEquals("After completing 10", 12, tracker.getHighestCommittable());
    }

    @Test
    public void testAllOffsetsCompleted() {
        // Scenario: All received offsets completed at once
        // received: {5, 6, 7, 8}
        // completed: all
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(5);
        tracker.markReceived(6);
        tracker.markReceived(7);
        tracker.markReceived(8);

        tracker.markComplete(5);
        tracker.markComplete(6);
        tracker.markComplete(7);
        tracker.markComplete(8);

        assertEquals("All completed", 8, tracker.getHighestCommittable());
    }

    @Test
    public void testNoOffsetsCompleted() {
        // Scenario: Received offsets but none completed yet
        // received: {0, 1, 2}
        // completed: none
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);

        assertEquals("No completions yet", -1, tracker.getHighestCommittable());
    }

    @Test
    public void testMultipleHoles() {
        // Scenario: Multiple holes in completion
        // received: {0, 1, 2, 3, 4, 5}
        // completed: {0, 2, 4} (1, 3, 5 missing)
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);
        tracker.markReceived(3);
        tracker.markReceived(4);
        tracker.markReceived(5);

        tracker.markComplete(0);
        assertEquals("After completing 0", 0, tracker.getHighestCommittable());

        tracker.markComplete(2);
        assertEquals("After completing 2 (1 is a hole)", 0, tracker.getHighestCommittable());

        tracker.markComplete(4);
        assertEquals("After completing 4 (1,3 are holes)", 0, tracker.getHighestCommittable());

        tracker.markComplete(1);
        assertEquals("After completing 1 (3 is still a hole)", 2, tracker.getHighestCommittable());

        tracker.markComplete(3);
        assertEquals("After completing 3 (5 is still a hole)", 4, tracker.getHighestCommittable());

        tracker.markComplete(5);
        assertEquals("After completing 5", 5, tracker.getHighestCommittable());
    }

    @Test
    public void testInterleavedReceiveAndComplete() {
        // Scenario: Receive and complete interleaved
        // Simulates continuous mode where records arrive and complete dynamically
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markComplete(0);
        assertEquals("Complete 0", 0, tracker.getHighestCommittable());

        tracker.markReceived(2);
        tracker.markComplete(1);
        assertEquals("Complete 1", 1, tracker.getHighestCommittable());

        tracker.markReceived(3);
        tracker.markReceived(4);
        tracker.markComplete(4);
        assertEquals("Complete 4 (2,3 not done)", 1, tracker.getHighestCommittable());

        tracker.markComplete(2);
        assertEquals("Complete 2 (3 not done)", 2, tracker.getHighestCommittable());

        tracker.markComplete(3);
        assertEquals("Complete 3", 4, tracker.getHighestCommittable());
    }

    @Test
    public void testStateCleanup() {
        // Scenario: Verify old state is cleaned up after commit
        // After committing offset N, offsets ≤ N should be removed
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);

        tracker.markComplete(0);
        tracker.markComplete(1);
        tracker.markComplete(2);

        assertEquals("Complete 0,1,2", 2, tracker.getHighestCommittable());

        // Now mark new offsets - if cleanup worked, we shouldn't have stale state
        tracker.markReceived(10);
        tracker.markReceived(11);

        tracker.markComplete(10);
        assertEquals("Complete 10", 10, tracker.getHighestCommittable());

        tracker.markComplete(11);
        assertEquals("Complete 11", 11, tracker.getHighestCommittable());
    }

    @Test
    public void testEmptyTracker() {
        // Scenario: No offsets received or completed
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        assertEquals("Empty tracker", -1, tracker.getHighestCommittable());
    }

    @Test
    public void testSingleOffset() {
        // Scenario: Single offset received and completed
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(42);
        tracker.markComplete(42);

        assertEquals("Single offset", 42, tracker.getHighestCommittable());
    }

    @Test
    public void testHighestCommittableIsIdempotent() {
        // Scenario: Calling getHighestCommittable multiple times without changes
        // should return the same result and properly clean up state
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(1);
        tracker.markReceived(2);
        tracker.markComplete(0);
        tracker.markComplete(1);
        tracker.markComplete(2);

        assertEquals("First call", 2, tracker.getHighestCommittable());
        assertEquals("Second call (should still be 2, state cleaned)", 2, tracker.getHighestCommittable());

        // Add new offset and verify it works after cleanup
        tracker.markReceived(3);
        tracker.markComplete(3);
        assertEquals("After new offset", 3, tracker.getHighestCommittable());
    }

    @Test
    public void testMarkCompleteBeforeMarkReceived() {
        // Scenario: Edge case - mark offset complete before it's marked received
        // This shouldn't happen in practice but tracker should handle it gracefully
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        tracker.markReceived(0);
        tracker.markReceived(2);
        tracker.markComplete(0);
        tracker.markComplete(1); // 1 was never received
        tracker.markComplete(2);

        // Since 1 was never received, it's not in receivedOffsets, so not a hole
        assertEquals("Offset 1 not received, so 0,2 are committable", 2, tracker.getHighestCommittable());
    }

    @Test
    public void testLargeNumberOfOffsets() {
        // Scenario: Many offsets to verify performance is reasonable
        PartitionOffsetTracker tracker = new PartitionOffsetTracker();

        // Receive 1000 offsets
        for (int i = 0; i < 1000; i++) {
            tracker.markReceived(i);
        }

        // Complete them all except 500
        for (int i = 0; i < 1000; i++) {
            if (i != 500) {
                tracker.markComplete(i);
            }
        }

        assertEquals("Hole at 500", 499, tracker.getHighestCommittable());

        // Complete the hole
        tracker.markComplete(500);
        assertEquals("All complete", 999, tracker.getHighestCommittable());
    }
}
