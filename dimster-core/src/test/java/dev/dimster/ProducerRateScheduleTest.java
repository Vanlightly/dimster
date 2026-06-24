package dev.dimster;

import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProducerRateScheduleTest {

    @Test
    public void oscillatingScheduleStartsAtMinAndPeaksHalfwayThroughPeriod() {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.OSCILLATING;
        schedule.minRate = 1_000;
        schedule.maxRate = 100_000;
        schedule.periodSeconds = 60;

        assertEquals(1_000.0, schedule.firstRate(), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ZERO), 0.0001);
        assertEquals(100_000.0, schedule.rateAt(Duration.ofSeconds(30)), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ofSeconds(60)), 0.0001);
    }

    @Test
    public void repeatingScheduleSupportsSmoothRampsAndHolds() {
        ProducerRateSchedule schedule = repeatingExample();

        assertEquals(1_000.0, schedule.firstRate(), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ZERO), 0.0001);
        assertEquals(50_500.0, schedule.rateAt(Duration.ofSeconds(30)), 0.0001);
        assertEquals(100_000.0, schedule.rateAt(Duration.ofSeconds(60)), 0.0001);
        assertEquals(100_000.0, schedule.rateAt(Duration.ofSeconds(180)), 0.0001);
        assertEquals(50_500.0, schedule.rateAt(Duration.ofSeconds(210)), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ofSeconds(240)), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ofSeconds(540)), 0.0001);
        assertEquals(50_500.0, schedule.rateAt(Duration.ofSeconds(570)), 0.0001);
    }

    @Test
    public void copyDividedByScalesEveryRate() {
        ProducerRateSchedule divided = repeatingExample().copyDividedBy(2.0);

        assertEquals(500.0, divided.rateAt(Duration.ZERO), 0.0001);
        assertEquals(50_000.0, divided.maxConfiguredRate(), 0.0001);
        assertEquals(25_250.0, divided.rateAt(Duration.ofSeconds(30)), 0.0001);
    }

    @Test
    public void repeatingScheduleCanRampDownToZeroAndPause() {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.REPEATING;

        ProducerRateSchedule.Segment down = new ProducerRateSchedule.Segment();
        down.fromRate = 1_000.0;
        down.toRate = 0.0;
        down.durationSeconds = 10;

        ProducerRateSchedule.Segment paused = new ProducerRateSchedule.Segment();
        paused.rate = 0.0;
        paused.durationSeconds = 30;

        ProducerRateSchedule.Segment up = new ProducerRateSchedule.Segment();
        up.fromRate = 0.0;
        up.toRate = 1_000.0;
        up.durationSeconds = 10;

        schedule.segments = List.of(down, paused, up);

        assertEquals(1_000.0, schedule.maxConfiguredRate(), 0.0001);
        assertEquals(0.0, schedule.rateAt(Duration.ofSeconds(10)), 0.0001);
        assertEquals(0.0, schedule.rateAt(Duration.ofSeconds(20)), 0.0001);
        assertEquals(Duration.ofMillis(20_001), schedule.durationUntilNextPositiveRate(Duration.ofSeconds(20)));
    }

    @Test
    public void oscillatingScheduleCanStartAtZero() {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.OSCILLATING;
        schedule.minRate = 0;
        schedule.maxRate = 1_000;
        schedule.periodSeconds = 60;

        assertEquals(0.0, schedule.firstRate(), 0.0001);
        assertEquals(0.0, schedule.rateAt(Duration.ZERO), 0.0001);
        assertEquals(1_000.0, schedule.rateAt(Duration.ofSeconds(30)), 0.0001);
        assertEquals(Duration.ofMillis(1), schedule.durationUntilNextPositiveRate(Duration.ZERO));
    }

    private static ProducerRateSchedule repeatingExample() {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.REPEATING;

        ProducerRateSchedule.Segment up = new ProducerRateSchedule.Segment();
        up.fromRate = 1_000.0;
        up.toRate = 100_000.0;
        up.durationSeconds = 60;

        ProducerRateSchedule.Segment high = new ProducerRateSchedule.Segment();
        high.rate = 100_000.0;
        high.durationSeconds = 120;

        ProducerRateSchedule.Segment down = new ProducerRateSchedule.Segment();
        down.fromRate = 100_000.0;
        down.toRate = 1_000.0;
        down.durationSeconds = 60;

        ProducerRateSchedule.Segment low = new ProducerRateSchedule.Segment();
        low.rate = 1_000.0;
        low.durationSeconds = 300;

        schedule.segments = List.of(up, high, down, low);
        return schedule;
    }
}
