package dev.dimster.kafka;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tracks received and completed offsets for a single partition in CONTINUOUS mode.
 *
 * <p>Handles offset gap tracking where records complete out-of-order. Kafka naturally
 * has offset gaps due to transaction markers, aborted transactions, and compaction.
 *
 * <p>An offset is committable if ALL received offsets ≤ it are completed (no gaps in completions).
 *
 * <p><b>Thread safety:</b> Not thread-safe. Each consumer runs on a single poll loop thread
 * and has its own tracker per partition, so no synchronization is needed.
 */
class PartitionOffsetTracker {
    private final TreeSet<Long> receivedOffsets = new TreeSet<>();
    private final Set<Long> completedOffsets = new HashSet<>();
    private long lastCommittable = -1;

    /**
     * Mark offset as received from poll().
     * Kafka can have offset gaps (txn markers, aborted txns, compaction).
     */
    void markReceived(long offset) {
        receivedOffsets.add(offset);
    }

    /**
     * Mark offset as completed (processing finished).
     *
     * <p>Does not calculate highest committable - call {@link #getHighestCommittable()}
     * when you need it (e.g., at commit time).
     */
    void markComplete(long offset) {
        completedOffsets.add(offset);
    }

    /**
     * Get highest committable offset. Returns -1 if nothing committable yet.
     *
     * <p>Algorithm: Iterate received offsets in ascending order until we find
     * the first offset that is not completed (first hole). The offset immediately
     * before the hole is the highest committable. Clean up state for committed offsets.
     *
     * <p>Idempotent: subsequent calls return the last committable offset even after cleanup.
     */
    long getHighestCommittable() {
        long highest = lastCommittable;

        // Find the first hole in completed offsets
        for (var it = receivedOffsets.iterator(); it.hasNext(); ) {
            long candidate = it.next();

            if (!completedOffsets.contains(candidate)) {
                // Candidate is a hole - stop here
                break;
            } else {
                // This offset is completed, it's the new highest
                highest = candidate;
            }
        }

        // Clean up old state only if we found new committable offsets
        if (highest > lastCommittable) {
            receivedOffsets.headSet(highest, true).clear();
            final long highestCommittable = highest;
            completedOffsets.removeIf(o -> o <= highestCommittable);
            lastCommittable = highest;
        }

        return highest;
    }
}
