package dev.dimster.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility for stop-aware sleeping. Sleeps in 1-second chunks while checking
 * a stop flag between each chunk, so the caller wakes up within ~1 second
 * of a stop being requested rather than blocking for the full duration.
 */
public class Sleeper {

    private static final long CHUNK_MS = 1000;

    /**
     * Sleep for the requested duration, checking stopRequested every second.
     *
     * @param stopRequested flag checked between sleep chunks
     * @param millis        total sleep duration in milliseconds
     * @return true if stopRequested became true or the thread was interrupted
     *         (caller should exit early); false if the full sleep completed normally
     */
    public static boolean sleepOrStop(AtomicBoolean stopRequested, long millis, boolean throwEx) {
        long remaining = millis;
        while (remaining > 0) {
            if (stopRequested.get()) {
                if (throwEx)
                    throw new StopRequestedException();
                else
                    return true;
            }
            long chunk = Math.min(remaining, CHUNK_MS);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (throwEx)
                    throw new StopRequestedException();
                else
                    return true;
            }
            remaining -= chunk;
        }

        if (stopRequested.get() && throwEx)
            throw new StopRequestedException();
        else
            return stopRequested.get();
    }
}
