package dev.dimster.kafka;

import dev.dimster.utils.Sleeper;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates processing time for consumer message batches across three modes:
 * sequential (procConcurrency=1), constrained concurrency (procConcurrency>1),
 * and full parallelism (procConcurrency=-1).
 *
 * <p>Handles the aggregate sleep optimization for small processing times
 * (midpoint < 10ms) to reduce OS scheduling variance.
 */
public class ProcessingTimeSimulator {
    private final int procMsMin;
    private final int procMsMax;
    private final int procConcurrency;
    private final boolean useAggregate;

    public ProcessingTimeSimulator(int procMsMin, int procMsMax, int procConcurrency) {
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procConcurrency = procConcurrency;
        this.useAggregate = procMsMax > 0
                && ((procMsMin + procMsMax) / 2) < KafkaConsumerWrapper.AGGREGATE_PROC_TIME_THRESHOLD_MS;
    }

    /**
     * Process a batch of messages with simulated processing time.
     * Calls the messageProcessor for each message or range of messages,
     * and sleeps appropriately based on the concurrency mode.
     *
     * @param batchSize number of messages in the batch
     * @param closing signal to check for early termination
     * @param callback callback for recording processing metrics
     * @param messageProcessor callback to process each message (or range of messages)
     * @return total sleep time in milliseconds
     */
    public long processBatch(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            MessageProcessor messageProcessor) {

        if (batchSize == 0 || procMsMax == 0) {
            // No processing time simulation - just process messages
            messageProcessor.processMessages(0, batchSize);
            return 0;
        }

        if (procConcurrency == 1) {
            return processSequential(batchSize, closing, callback, messageProcessor);
        } else if (procConcurrency > 1) {
            return processConstrained(batchSize, closing, callback, messageProcessor);
        } else { // procConcurrency == -1
            return processFullParallel(batchSize, closing, callback, messageProcessor);
        }
    }

    /**
     * Sequential processing: process messages one by one, sleeping after each.
     * This matches the existing behavior when procConcurrency=1.
     */
    private long processSequential(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            MessageProcessor messageProcessor) {

        long totalSleepMs = 0;

        for (int i = 0; i < batchSize; i++) {
            // Process this single message
            messageProcessor.processMessages(i, i + 1);

            // Generate and apply processing time
            long sleepMs = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);

            if (useAggregate) {
                // Defer sleep to end
                totalSleepMs += sleepMs;
            } else {
                // Sleep immediately and record metric
                long startNanos = System.nanoTime();
                if (Sleeper.sleepOrStop(closing, sleepMs, false)) {
                    return totalSleepMs; // Early exit
                }
                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
                callback.processingTimeCompleted(latencyMicros);
            }
        }

        // Aggregate sleep at end if enabled
        if (useAggregate && totalSleepMs > 0) {
            Sleeper.sleepOrStop(closing, totalSleepMs, false);
        }

        return totalSleepMs;
    }

    /**
     * Constrained concurrency: divide messages into chunks of size procConcurrency,
     * sleep for the max processing time in each chunk.
     */
    private long processConstrained(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            MessageProcessor messageProcessor) {

        // Generate processing times for all messages
        long[] processingTimes = new long[batchSize];
        for (int i = 0; i < batchSize; i++) {
            processingTimes[i] = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
        }

        long totalSleepMs = 0;
        int numChunks = (batchSize + procConcurrency - 1) / procConcurrency;

        for (int chunk = 0; chunk < numChunks; chunk++) {
            int startIdx = chunk * procConcurrency;
            int endIdx = Math.min(startIdx + procConcurrency, batchSize);

            // Find max processing time in this chunk
            long maxTime = 0;
            for (int i = startIdx; i < endIdx; i++) {
                maxTime = Math.max(maxTime, processingTimes[i]);
            }

            // Process messages in this chunk
            messageProcessor.processMessages(startIdx, endIdx);

            // Sleep for chunk duration
            if (useAggregate) {
                totalSleepMs += maxTime;
            } else {
                long startNanos = System.nanoTime();
                if (Sleeper.sleepOrStop(closing, maxTime, false)) {
                    return totalSleepMs; // Early exit
                }
                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
                callback.processingTimeCompleted(latencyMicros);
            }
        }

        // Aggregate sleep at end if enabled
        if (useAggregate && totalSleepMs > 0) {
            Sleeper.sleepOrStop(closing, totalSleepMs, false);
        }

        return totalSleepMs;
    }

    /**
     * Full parallelism: all messages process concurrently,
     * sleep once for the maximum processing time.
     */
    private long processFullParallel(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            MessageProcessor messageProcessor) {

        // Generate processing times and find max
        long maxTime = 0;
        for (int i = 0; i < batchSize; i++) {
            long procTime = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
            maxTime = Math.max(maxTime, procTime);
        }

        // Process all messages
        messageProcessor.processMessages(0, batchSize);

        // Sleep once for max duration
        if (!useAggregate) {
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, maxTime, false)) {
                return 0; // Early exit
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
        }

        // In aggregate mode, just track the time (actual sleep happens in wrapper)
        long totalSleepMs = maxTime;

        // Aggregate sleep at end if enabled
        if (useAggregate && totalSleepMs > 0) {
            Sleeper.sleepOrStop(closing, totalSleepMs, false);
        }

        return totalSleepMs;
    }

    /**
     * Callback interface for processing messages in a range.
     * The processor should handle messages from startIdx (inclusive) to endIdx (exclusive).
     */
    @FunctionalInterface
    public interface MessageProcessor {
        /**
         * Process messages in the range [startIdx, endIdx).
         *
         * @param startIdx first message index to process (inclusive)
         * @param endIdx last message index + 1 (exclusive)
         */
        void processMessages(int startIdx, int endIdx);
    }
}
