package dev.dimster.utils;

import static dev.dimster.utils.UniformRateLimiter.uninterruptibleSleepNs;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class UniformRateLimiterTest {

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroRate() {
        new UniformRateLimiter(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeRate() {
        new UniformRateLimiter(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNaN() {
        new UniformRateLimiter(Double.NaN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInfinity() {
        new UniformRateLimiter(Double.POSITIVE_INFINITY);
    }

    @Test
    public void intervalMatchesRate() {
        UniformRateLimiter limiter = new UniformRateLimiter(1000);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(1), limiter.getIntervalNs());

        limiter = new UniformRateLimiter(1);
        assertEquals(TimeUnit.SECONDS.toNanos(1), limiter.getIntervalNs());
    }

    @Test
    public void acquireReturnsUniformlySpacedTimes() {
        double rate = 10_000;
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        long expectedInterval = limiter.getIntervalNs();

        long first = limiter.acquire();
        long second = limiter.acquire();
        long third = limiter.acquire();

        assertEquals(expectedInterval, second - first);
        assertEquals(expectedInterval, third - second);
    }

    @Test
    public void acquireTimesAreMonotonicallyIncreasing() {
        UniformRateLimiter limiter = new UniformRateLimiter(100_000);

        long prev = limiter.acquire();
        for (int i = 0; i < 1000; i++) {
            long next = limiter.acquire();
            assertTrue("acquire() times must be monotonically increasing", next > prev);
            prev = next;
        }
    }

    @Test
    public void firstAcquireAnchorsToCurrentTime() {
        long before = System.nanoTime();
        UniformRateLimiter limiter = new UniformRateLimiter(1000);
        long firstAcquire = limiter.acquire();
        long after = System.nanoTime();

        // First acquire should return a time close to now (the start anchor)
        assertTrue("first acquire should be >= time before creation", firstAcquire >= before);
        assertTrue("first acquire should be <= time after acquire", firstAcquire <= after);
    }

    @Test
    public void multipleThreadsGetDistinctSlots() throws Exception {
        int threadCount = 10;
        int acquiresPerThread = 100;
        int totalAcquires = threadCount * acquiresPerThread;

        UniformRateLimiter limiter = new UniformRateLimiter(100_000);

        List<Long> allTimes = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            Thread thread = Thread.ofPlatform().start(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { throw new RuntimeException(e); }

                for (int i = 0; i < acquiresPerThread; i++) {
                    allTimes.add(limiter.acquire());
                }
            });
            threads.add(thread);
        }

        ready.await();
        go.countDown();

        for (Thread thread : threads) {
            thread.join(5000);
        }

        assertEquals(totalAcquires, allTimes.size());

        // All times should be unique (each acquire gets its own slot)
        long distinctCount = allTimes.stream().distinct().count();
        assertEquals("every acquire() must return a unique time slot", totalAcquires, (int) distinctCount);

        // Times should be uniformly spaced when sorted
        List<Long> sorted = new ArrayList<>(allTimes);
        Collections.sort(sorted);
        long expectedInterval = limiter.getIntervalNs();
        for (int i = 1; i < sorted.size(); i++) {
            assertEquals("intervals between sorted slots must be uniform",
                    expectedInterval, sorted.get(i) - sorted.get(i - 1));
        }
    }

    @Test
    public void multipleThreadsNoLostAcquires() throws Exception {
        int threadCount = 50;
        int acquiresPerThread = 200;
        int totalAcquires = threadCount * acquiresPerThread;

        UniformRateLimiter limiter = new UniformRateLimiter(1_000_000);

        AtomicLong acquireCount = new AtomicLong();
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            Thread.ofPlatform().start(() -> {
                for (int i = 0; i < acquiresPerThread; i++) {
                    limiter.acquire();
                    acquireCount.incrementAndGet();
                }
                done.countDown();
            });
        }

        assertTrue("all threads should complete within 5 seconds", done.await(5, TimeUnit.SECONDS));
        assertEquals(totalAcquires, acquireCount.get());
    }

    @Test
    public void actualThroughputApproximatesTargetRate() throws Exception {
        double targetRate = 50_000;
        UniformRateLimiter limiter = new UniformRateLimiter(targetRate);
        int totalOps = 5_000;

        long startNs = System.nanoTime();
        for (int i = 0; i < totalOps; i++) {
            long intended = limiter.acquire();
            uninterruptibleSleepNs(intended);
        }
        long elapsedNs = System.nanoTime() - startNs;

        double elapsedSec = elapsedNs / 1_000_000_000.0;
        double actualRate = totalOps / elapsedSec;

        // Allow 10% tolerance
        assertTrue("actual rate " + actualRate + " should be within 10% of target " + targetRate,
                actualRate > targetRate * 0.9 && actualRate < targetRate * 1.1);
    }

    @Test
    public void multipleThreadsThroughputApproximatesTargetRate() throws Exception {
        double targetRate = 100_000;
        int threadCount = 5;
        int opsPerThread = 4_000;
        int totalOps = threadCount * opsPerThread;

        UniformRateLimiter limiter = new UniformRateLimiter(targetRate);
        CountDownLatch done = new CountDownLatch(threadCount);

        long startNs = System.nanoTime();
        for (int t = 0; t < threadCount; t++) {
            Thread.ofPlatform().start(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    long intended = limiter.acquire();
                    uninterruptibleSleepNs(intended);
                }
                done.countDown();
            });
        }

        assertTrue("all threads should complete within 5 seconds", done.await(5, TimeUnit.SECONDS));
        long elapsedNs = System.nanoTime() - startNs;

        double elapsedSec = elapsedNs / 1_000_000_000.0;
        double actualRate = totalOps / elapsedSec;

        // Allow 10% tolerance
        assertTrue("actual rate " + actualRate + " should be within 10% of target " + targetRate,
                actualRate > targetRate * 0.9 && actualRate < targetRate * 1.1);
    }

    @Test
    public void uninterruptibleSleepNsInThePastReturnsImmediately() {
        long before = System.nanoTime();
        // Intended time is in the past — should not sleep
        uninterruptibleSleepNs(before - TimeUnit.SECONDS.toNanos(1));
        long elapsed = System.nanoTime() - before;

        assertTrue("sleeping to a past time should return in under 1ms",
                elapsed < TimeUnit.MILLISECONDS.toNanos(1));
    }
}
