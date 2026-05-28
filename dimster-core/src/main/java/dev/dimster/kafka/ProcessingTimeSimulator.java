package dev.dimster.kafka;

import dev.dimster.utils.Sleeper;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates processing time for consumer message batches across three modes:
 * sequential (procParallelism=1), constrained concurrency (procParallelism>1),
 * and full parallelism (procParallelism=-1).
 *
 * <p>Handles the aggregate sleep optimization for small processing times
 * (midpoint < 10ms) to reduce OS scheduling variance.
 */
public class ProcessingTimeSimulator {
    private final int procMsMin;
    private final int procMsMax;
    private final int procParallelism;

    public ProcessingTimeSimulator(int procMsMin, int procMsMax, int procParallelism) {
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procParallelism = procParallelism;
    }

    /**
     * Process a batch of messages with simulated processing time.
     * Calls the messageProcessor for each message with its simulated processing start time,
     * and sleeps appropriately based on the concurrency mode.
     *
     * @param batchSize number of messages in the batch
     * @param closing signal to check for early termination
     * @param callback callback for recording processing metrics
     * @param deliveryTime when consumer.poll() returned (base time for calculating processing start times)
     * @param messageProcessor callback to process each message with its simulated start time
     * @return total sleep time in milliseconds
     */
    public long processBatch(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        if (batchSize == 0 || procMsMax == 0) {
            // No processing time simulation - process messages with deliveryTime
            for (int i = 0; i < batchSize; i++) {
                messageProcessor.processMessage(i, deliveryTime);
            }
            return 0;
        }

        if (procParallelism == 1) {
            return processSequential(batchSize, closing, callback, deliveryTime, messageProcessor);
        } else if (procParallelism > 1) {
            return processConstrained(batchSize, closing, callback, deliveryTime, messageProcessor);
        } else { // procParallelism == -1
            return processFullParallel(batchSize, closing, callback, deliveryTime, messageProcessor);
        }
    }

    /**
     * Sequential processing: process messages one by one.
     * Each message starts after all prior messages have finished.
     */
    private long processSequential(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        long cumulativeMs = 0;

        for (int i = 0; i < batchSize; i++) {
            // This message starts after all prior messages finish
            Instant startTime = deliveryTime.plusMillis(cumulativeMs);

            // Process this message with its start time
            messageProcessor.processMessage(i, startTime);

            // Generate processing time and add to cumulative
            long procMs = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
            cumulativeMs += procMs;
        }

        // Sleep once for total time
        if (cumulativeMs > 0) {
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, cumulativeMs, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
        }

        return cumulativeMs;
    }

    /**
     * Constrained concurrency: simulate N concurrent workers, each processing their
     * assigned records serially. Sleep once for the slowest worker's total time.
     */
    private long processConstrained(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        // Generate processing times for all messages
        long[] processingTimes = new long[batchSize];
        for (int i = 0; i < batchSize; i++) {
            processingTimes[i] = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
        }

        // Distribute records among workers and process each worker's queue
        int recordsPerWorker = (batchSize + procParallelism - 1) / procParallelism;
        long maxWorkerTime = 0;

        for (int worker = 0; worker < procParallelism; worker++) {
            int startIdx = worker * recordsPerWorker;
            int endIdx = Math.min(startIdx + recordsPerWorker, batchSize);

            if (startIdx >= batchSize) {
                break; // No more records for this worker
            }

            // Process each message in this worker's queue
            long workerCumulativeMs = 0;
            for (int i = startIdx; i < endIdx; i++) {
                // Message starts after all prior messages in this worker's queue finish
                Instant startTime = deliveryTime.plusMillis(workerCumulativeMs);
                messageProcessor.processMessage(i, startTime);

                // Add this message's processing time to worker cumulative
                workerCumulativeMs += processingTimes[i];
            }

            maxWorkerTime = Math.max(maxWorkerTime, workerCumulativeMs);
        }

        // Sleep once for the slowest worker's time (simulating concurrent workers finishing)
        if (maxWorkerTime > 0) {
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, maxWorkerTime, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
        }

        return maxWorkerTime;
    }

    /**
     * Full parallelism: all messages process concurrently (start at same time),
     * sleep once for the maximum processing time.
     */
    private long processFullParallel(
            int batchSize,
            AtomicBoolean closing,
            ConsumerCallback callback,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        // Generate processing times and process all messages (all start at deliveryTime)
        long maxTime = 0;
        for (int i = 0; i < batchSize; i++) {
            long procTime = (procMsMin == procMsMax)
                    ? procMsMin
                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
            maxTime = Math.max(maxTime, procTime);

            // All messages start at deliveryTime (concurrent)
            messageProcessor.processMessage(i, deliveryTime);
        }

        // Sleep once for max duration
        if (maxTime > 0) {
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, maxTime, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
        }

        return maxTime;
    }

    /**
     * Callback interface for processing a single message with its simulated processing start time.
     */
    @FunctionalInterface
    public interface MessageProcessor {
        /**
         * Process a single message with its simulated processing start time.
         *
         * @param messageIdx index of the message in the batch
         * @param simulatedProcessingStartTime when this message starts processing (for e2e latency calculation)
         */
        void processMessage(int messageIdx, Instant simulatedProcessingStartTime);
    }
}
