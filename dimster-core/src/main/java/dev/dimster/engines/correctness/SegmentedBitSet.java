package dev.dimster.engines.correctness;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.BitSet;

/**
 * A bit set that supports long indices by segmenting into fixed-size BitSet chunks.
 * This avoids BitSet's Integer.MAX_VALUE ceiling and keeps memory proportional
 * to the actual range of values stored.
 *
 * Thread safety: callers must synchronize externally.
 */
final class SegmentedBitSet {

    static final int SEGMENT_SIZE = 1_000_000;

    private final TreeMap<Long, BitSet> segments = new TreeMap<>();

    void set(long index) {
        long segmentKey = index / SEGMENT_SIZE;
        int offset = (int) (index % SEGMENT_SIZE);
        segments.computeIfAbsent(segmentKey, k -> new BitSet()).set(offset);
    }

    boolean get(long index) {
        long segmentKey = index / SEGMENT_SIZE;
        int offset = (int) (index % SEGMENT_SIZE);
        BitSet segment = segments.get(segmentKey);
        return segment != null && segment.get(offset);
    }

    /**
     * Find all gaps (unset bits) in the range [0, maxInclusive].
     * Returns a list of [from, to] ranges (inclusive) where bits are not set.
     *
     * Uses BitSet.nextClearBit/nextSetBit to skip over contiguous regions,
     * making this O(gaps + segments) rather than O(maxInclusive).
     */
    List<long[]> findGaps(long maxInclusive) {
        List<long[]> gaps = new ArrayList<>();
        long gapStart = -1;
        long pos = 0;

        while (pos <= maxInclusive) {
            long segmentKey = pos / SEGMENT_SIZE;
            BitSet segment = segments.get(segmentKey);
            long segmentBase = segmentKey * SEGMENT_SIZE;
            long segmentEnd = Math.min(segmentBase + SEGMENT_SIZE - 1, maxInclusive);

            if (segment == null) {
                // Entire segment is missing — start or extend a gap
                if (gapStart == -1) gapStart = pos;
                // Skip to end of consecutive missing segments
                Map.Entry<Long, BitSet> nextEntry = segments.higherEntry(segmentKey);
                if (nextEntry != null) {
                    pos = Math.min(nextEntry.getKey() * SEGMENT_SIZE, maxInclusive + 1);
                } else {
                    pos = maxInclusive + 1;
                }
                continue;
            }

            // Scan within this segment using nextClearBit/nextSetBit
            int offset = (int) (pos - segmentBase);
            int segLimit = (int) (segmentEnd - segmentBase);

            while (offset <= segLimit) {
                int clearAt = segment.nextClearBit(offset);
                if (clearAt > segLimit) {
                    // Rest of segment is set — close any open gap
                    if (gapStart != -1) {
                        gaps.add(new long[]{gapStart, segmentBase + offset - 1});
                        gapStart = -1;
                    }
                    break;
                }

                // Found a clear bit — close any preceding set run and start/extend gap
                if (gapStart == -1) gapStart = segmentBase + clearAt;

                int setAt = segment.nextSetBit(clearAt);
                if (setAt == -1 || setAt > segLimit) {
                    // Gap extends to end of segment — leave it open
                    offset = segLimit + 1;
                } else {
                    // Gap ends before a set bit — close it
                    gaps.add(new long[]{gapStart, segmentBase + setAt - 1});
                    gapStart = -1;
                    offset = setAt;
                }
            }

            pos = segmentEnd + 1;
        }

        // Close any trailing gap
        if (gapStart != -1) {
            gaps.add(new long[]{gapStart, maxInclusive});
        }

        return gaps;
    }

    long cardinality() {
        long count = 0;
        for (BitSet segment : segments.values()) {
            count += segment.cardinality();
        }
        return count;
    }
}
