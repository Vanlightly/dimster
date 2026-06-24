package dev.dimster.utils;

import dev.dimster.ProducerRateSchedule;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * Rate limiter for workload-defined variable producer rates.
 *
 * It has the same shared-limiter role as {@link UniformRateLimiter}: all
 * producer threads on a worker acquire slots from one virtual timeline. The
 * difference is that each interval is calculated from the schedule at the
 * current intended send time, so ramps and oscillations remain smooth even
 * when several producer threads share the limiter.
 */
public final class ScheduledRateLimiter {

    private static final AtomicLongFieldUpdater<ScheduledRateLimiter> START_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ScheduledRateLimiter.class, "start");
    private static final AtomicLongFieldUpdater<ScheduledRateLimiter> NEXT_TIME_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ScheduledRateLimiter.class, "nextTime");
    private static final double ONE_SEC_IN_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long MAX_FUTURE_RESERVATION_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RATE_RECHECK_NS = TimeUnit.MILLISECONDS.toNanos(100);

    private final ProducerRateSchedule schedule;
    private final Long startEpochMillis;
    private volatile long start = Long.MIN_VALUE;
    private volatile long nextTime = Long.MIN_VALUE;

    public ScheduledRateLimiter(ProducerRateSchedule schedule) {
        this(schedule, null);
    }

    public ScheduledRateLimiter(ProducerRateSchedule schedule, Long startEpochMillis) {
        if (schedule == null) {
            throw new IllegalArgumentException("schedule cannot be null");
        }
        schedule.validate();
        this.schedule = schedule;
        this.startEpochMillis = startEpochMillis;
    }

    public ProducerRateSchedule schedule() {
        return schedule;
    }

    /**
     * Reserve and return the next intended send time. The CAS loop lets many
     * producer threads share the same limiter without serializing on a lock.
     */
    public long acquire() {
        long startNanos = ensureStarted();
        while (true) {
            long current = nextTime;
            if (current == Long.MIN_VALUE) {
                if (NEXT_TIME_UPDATER.compareAndSet(this, Long.MIN_VALUE, startNanos)) {
                    current = startNanos;
                } else {
                    continue;
                }
            }

            long now = System.nanoTime();
            long base = Math.max(current, now);
            Duration elapsed = elapsedAt(base, startNanos);
            double rate = schedule.rateAt(elapsed);
            if (rate <= 0.0) {
                parkBeforeRecheck(schedule.durationUntilNextPositiveRate(elapsed).toNanos());
                continue;
            }
            long intervalNs = Math.max(1L, Math.round(ONE_SEC_IN_NS / rate));
            if (intervalNs > MAX_FUTURE_RESERVATION_NS) {
                // Near-zero ramps can otherwise reserve a send time minutes
                // into the future. Recheck shortly so the limiter follows the
                // ramp as the wall-clock rate becomes materially positive.
                parkBeforeRecheck(intervalNs);
                continue;
            }

            long next = base + intervalNs;
            if (NEXT_TIME_UPDATER.compareAndSet(this, current, next)) {
                return base;
            }
        }
    }

    private long ensureStarted() {
        long existing = start;
        if (existing != Long.MIN_VALUE) {
            return existing;
        }
        long now = System.nanoTime();
        if (START_UPDATER.compareAndSet(this, Long.MIN_VALUE, now)) {
            return now;
        }
        return start;
    }

    private Duration elapsedAt(long intendedSendTimeNanos, long localStartNanos) {
        if (startEpochMillis != null) {
            long elapsedMillis = System.currentTimeMillis() - startEpochMillis;
            return Duration.ofMillis(Math.max(0L, elapsedMillis));
        }
        return Duration.ofNanos(Math.max(0L, intendedSendTimeNanos - localStartNanos));
    }

    private static void parkBeforeRecheck(long requestedWaitNs) {
        long waitNs = Math.max(1L, Math.min(requestedWaitNs, RATE_RECHECK_NS));
        LockSupport.parkNanos(waitNs);
    }
}
