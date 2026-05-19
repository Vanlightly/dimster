package dev.dimster.utils;

import dev.dimster.utils.StopRequestedException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class SleeperTest {

    @Test
    public void fullSleepReturnsFalse() {
        AtomicBoolean stop = new AtomicBoolean(false);
        long start = System.currentTimeMillis();
        boolean result = Sleeper.sleepOrStop(stop, 200, false);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse("should return false when sleep completes normally", result);
        assertTrue("should sleep for approximately the requested duration", elapsed >= 180);
    }

    @Test
    public void stopBeforeSleepReturnsTrue() {
        AtomicBoolean stop = new AtomicBoolean(true);
        long start = System.currentTimeMillis();
        boolean result = Sleeper.sleepOrStop(stop, 5000, false);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("should return true when stopRequested is already set", result);
        assertTrue("should return immediately when already stopped", elapsed < 500);
    }

    @Test
    public void stopDuringSleepReturnsTrue() throws InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread testThread = new Thread(() -> {
            boolean result = Sleeper.sleepOrStop(stop, 10_000, false);
            assertTrue("should return true when stop is set mid-sleep", result);
        });
        testThread.start();

        // Set stop after a short delay
        Thread.sleep(300);
        stop.set(true);

        // Should wake up within ~1 second of stop being set
        testThread.join(3000);
        assertFalse("sleeper should have woken up within ~1 second", testThread.isAlive());
    }

    @Test
    public void zeroMillisSleepReturnsFalse() {
        AtomicBoolean stop = new AtomicBoolean(false);
        boolean result = Sleeper.sleepOrStop(stop, 0, false);
        assertFalse("zero-duration sleep should return false", result);
    }

    @Test(expected = StopRequestedException.class)
    public void throwExThrowsWhenAlreadyStopped() {
        AtomicBoolean stop = new AtomicBoolean(true);
        Sleeper.sleepOrStop(stop, 5000, true);
    }

    @Test(expected = StopRequestedException.class)
    public void throwExThrowsWhenStopSetDuringSleep() throws InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);

        // Set stop after a short delay from another thread
        Thread setter = new Thread(() -> {
            try { Thread.sleep(300); } catch (InterruptedException ignored) {}
            stop.set(true);
        });
        setter.start();

        try {
            Sleeper.sleepOrStop(stop, 10_000, true);
        } finally {
            setter.join(3000);
        }
    }

    @Test
    public void throwExDoesNotThrowWhenSleepCompletesNormally() {
        AtomicBoolean stop = new AtomicBoolean(false);
        // Should complete without throwing
        boolean result = Sleeper.sleepOrStop(stop, 100, true);
        assertFalse("should return false when sleep completes normally with throwEx=true", result);
    }

    @Test
    public void shortSleepWorksCorrectly() {
        AtomicBoolean stop = new AtomicBoolean(false);
        long start = System.currentTimeMillis();
        boolean result = Sleeper.sleepOrStop(stop, 50, false);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result);
        assertTrue("short sleep should complete in approximately the requested time", elapsed >= 40);
    }
}
