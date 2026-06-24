package dev.dimster.utils;

import dev.dimster.ProducerRateSchedule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ScheduledRateLimiterTest {

    @Test
    public void zeroToPositiveRampDoesNotReserveFarFutureSendTimes() {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.REPEATING;

        ProducerRateSchedule.Segment ramp = new ProducerRateSchedule.Segment();
        ramp.fromRate = 0.0;
        ramp.toRate = 1_000.0;
        ramp.durationSeconds = 1;

        ProducerRateSchedule.Segment hold = new ProducerRateSchedule.Segment();
        hold.rate = 1_000.0;
        hold.durationSeconds = 10;

        schedule.segments = List.of(ramp, hold);

        ScheduledRateLimiter limiter = new ScheduledRateLimiter(schedule);
        long before = System.nanoTime();
        long first = limiter.acquire();
        long second = limiter.acquire();

        assertTrue("first send should not be parked far into the future",
                first - before < TimeUnit.SECONDS.toNanos(1));
        assertTrue("near-zero ramp rate should not poison the next reservation",
                second - first < TimeUnit.SECONDS.toNanos(1));
    }
}
