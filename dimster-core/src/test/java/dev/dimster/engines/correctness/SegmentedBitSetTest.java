package dev.dimster.engines.correctness;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class SegmentedBitSetTest {

    @Test
    public void noGapsWhenAllSet() {
        SegmentedBitSet bs = new SegmentedBitSet();
        for (long i = 0; i < 100; i++) {
            bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(99);
        assertTrue(gaps.isEmpty());
    }

    @Test
    public void allGapsWhenNoneSet() {
        SegmentedBitSet bs = new SegmentedBitSet();
        List<long[]> gaps = bs.findGaps(99);
        assertEquals(1, gaps.size());
        assertEquals(0, gaps.get(0)[0]);
        assertEquals(99, gaps.get(0)[1]);
    }

    @Test
    public void singleGapInMiddle() {
        SegmentedBitSet bs = new SegmentedBitSet();
        for (long i = 0; i < 10; i++) {
            if (i != 5) bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(9);
        assertEquals(1, gaps.size());
        assertEquals(5, gaps.get(0)[0]);
        assertEquals(5, gaps.get(0)[1]);
    }

    @Test
    public void multipleGaps() {
        SegmentedBitSet bs = new SegmentedBitSet();
        // Set 0-2, skip 3-4, set 5-6, skip 7, set 8-9
        for (long i : new long[]{0, 1, 2, 5, 6, 8, 9}) {
            bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(9);
        assertEquals(2, gaps.size());
        assertEquals(3, gaps.get(0)[0]);
        assertEquals(4, gaps.get(0)[1]);
        assertEquals(7, gaps.get(1)[0]);
        assertEquals(7, gaps.get(1)[1]);
    }

    @Test
    public void gapAtStart() {
        SegmentedBitSet bs = new SegmentedBitSet();
        for (long i = 5; i < 10; i++) {
            bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(9);
        assertEquals(1, gaps.size());
        assertEquals(0, gaps.get(0)[0]);
        assertEquals(4, gaps.get(0)[1]);
    }

    @Test
    public void gapAtEnd() {
        SegmentedBitSet bs = new SegmentedBitSet();
        for (long i = 0; i < 5; i++) {
            bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(9);
        assertEquals(1, gaps.size());
        assertEquals(5, gaps.get(0)[0]);
        assertEquals(9, gaps.get(0)[1]);
    }

    @Test
    public void crossSegmentBoundary() {
        SegmentedBitSet bs = new SegmentedBitSet();
        int seg = SegmentedBitSet.SEGMENT_SIZE;

        // Set values in first and second segments with a gap spanning the boundary
        for (long i = 0; i < seg - 5; i++) {
            bs.set(i);
        }
        for (long i = seg + 5; i < seg + 20; i++) {
            bs.set(i);
        }

        List<long[]> gaps = bs.findGaps(seg + 19);
        assertEquals(1, gaps.size());
        assertEquals(seg - 5, gaps.get(0)[0]);
        assertEquals(seg + 4, gaps.get(0)[1]);
    }

    @Test
    public void entireMissingSegment() {
        SegmentedBitSet bs = new SegmentedBitSet();
        int seg = SegmentedBitSet.SEGMENT_SIZE;

        // Set values in segment 0 and segment 2, skip segment 1 entirely
        for (long i = 0; i < 10; i++) {
            bs.set(i);
        }
        for (long i = 2L * seg; i < 2L * seg + 10; i++) {
            bs.set(i);
        }

        List<long[]> gaps = bs.findGaps(2L * seg + 9);
        // Gap from 10 to 2*seg-1 (covers end of seg0 + all of seg1)
        // then no gap in the set portion of seg2
        assertEquals(1, gaps.size());
        assertEquals(10, gaps.get(0)[0]);
        assertEquals(2L * seg - 1, gaps.get(0)[1]);
    }

    @Test
    public void multipleMissingSegments() {
        SegmentedBitSet bs = new SegmentedBitSet();
        int seg = SegmentedBitSet.SEGMENT_SIZE;

        // Only set values in segment 0 and segment 3
        for (long i = 0; i < seg; i++) {
            bs.set(i);
        }
        for (long i = 3L * seg; i < 3L * seg + seg; i++) {
            bs.set(i);
        }

        List<long[]> gaps = bs.findGaps(4L * seg - 1);
        // Gap: segments 1-2 entirely, then gap at segment 4 (doesn't exist)
        // Actually: seg0 full, seg1+2 missing, seg3 full
        // maxInclusive = 4*seg-1, so no seg4
        assertEquals(1, gaps.size());
        assertEquals((long) seg, gaps.get(0)[0]);
        assertEquals(3L * seg - 1, gaps.get(0)[1]);
    }

    @Test
    public void maxInclusiveMidSegment() {
        SegmentedBitSet bs = new SegmentedBitSet();
        int seg = SegmentedBitSet.SEGMENT_SIZE;

        // Set first 10 values in segment 0
        for (long i = 0; i < 10; i++) {
            bs.set(i);
        }
        // Set some in segment 1
        bs.set((long) seg + 5);

        // maxInclusive stops mid-segment 1
        List<long[]> gaps = bs.findGaps(seg + 3);
        // Gap from 10 to seg+3 (seg+5 is beyond maxInclusive)
        assertEquals(1, gaps.size());
        assertEquals(10, gaps.get(0)[0]);
        assertEquals((long) seg + 3, gaps.get(0)[1]);
    }

    @Test
    public void singleElement() {
        SegmentedBitSet bs = new SegmentedBitSet();
        bs.set(0);
        List<long[]> gaps = bs.findGaps(0);
        assertTrue(gaps.isEmpty());
    }

    @Test
    public void singleElementNotSet() {
        SegmentedBitSet bs = new SegmentedBitSet();
        List<long[]> gaps = bs.findGaps(0);
        assertEquals(1, gaps.size());
        assertEquals(0, gaps.get(0)[0]);
        assertEquals(0, gaps.get(0)[1]);
    }

    @Test
    public void largeScaleNoGaps() {
        SegmentedBitSet bs = new SegmentedBitSet();
        long count = 3_000_000L; // spans 3 segments
        for (long i = 0; i < count; i++) {
            bs.set(i);
        }
        List<long[]> gaps = bs.findGaps(count - 1);
        assertTrue(gaps.isEmpty());
        assertEquals(count, bs.cardinality());
    }

    @Test
    public void largeScaleWithGaps() {
        SegmentedBitSet bs = new SegmentedBitSet();
        long count = 3_000_000L;
        // Skip every millionth message (one per segment boundary area)
        for (long i = 0; i < count; i++) {
            if (i % 1_000_000 != 500_000) {
                bs.set(i);
            }
        }
        List<long[]> gaps = bs.findGaps(count - 1);
        assertEquals(3, gaps.size());
        for (long[] gap : gaps) {
            assertEquals(gap[0], gap[1]); // each gap is a single missing value
        }
    }
}
