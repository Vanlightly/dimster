package dev.dimster.engines.correctness;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe correctness tracker that detects data loss, duplicates,
 * out-of-order delivery, and CRC failures.
 *
 * <p>All checks are scoped per producer stream. Each producer creates a
 * monotonically increasing sequence of messages keyed by its producer ID,
 * so all messages from one producer land on the same partition. The
 * partition is an implementation detail — correctness is about the
 * producer stream.</p>
 *
 * <p>Requires {@code consumerGroupsPerTopic == 1}. Multiple consumer
 * groups would cause each message to be consumed once per group, which
 * the checker would see as duplicates.</p>
 *
 * <p>Design note: the consumer may receive a message before the producer
 * receives the ack callback. Therefore, the consumer side never checks
 * against the "sent" set in real-time. Loss detection only happens at
 * finalization, after all acks are guaranteed complete.</p>
 */
public class ModelChecker {

    private static final Logger log = LoggerFactory.getLogger(ModelChecker.class);
    private static final int MAX_VIOLATIONS = 1000;

    private final boolean detectDuplicates;
    private final boolean detectOutOfOrder;
    private final int logDuplicateCount;

    // Producer-side: highest sent sequence per producer, and producer-to-topic mapping
    private final ConcurrentHashMap<Integer, AtomicLong> sentSequences = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> producerTopics = new ConcurrentHashMap<>();

    // Consumer-side: received sequences per producer (synchronized per-producer)
    private final ConcurrentHashMap<Integer, SegmentedBitSet> receivedSequences = new ConcurrentHashMap<>();

    // Per-producer: highest received sequence (for out-of-order detection)
    private final ConcurrentHashMap<Integer, AtomicLong> producerHighSequence = new ConcurrentHashMap<>();

    // Violation counters
    private final LongAdder duplicateCount = new LongAdder();
    private final LongAdder outOfOrderCount = new LongAdder();
    private final LongAdder crcFailureCount = new LongAdder();
    private final LongAdder totalSent = new LongAdder();
    private final LongAdder totalReceived = new LongAdder();

    // Violation timeline (capped)
    private final ConcurrentLinkedQueue<Violation> violations = new ConcurrentLinkedQueue<>();

    public sealed interface Violation permits DuplicateViolation, OutOfOrderViolation, CrcFailureViolation {}

    public record DuplicateViolation(int producerId, long sequence, String topic, int partition,
                                     long highSequence, long timestampMs) implements Violation {}

    public record OutOfOrderViolation(int producerId, long sequence, long highSequence,
                                      String topic, int partition, long timestampMs) implements Violation {}

    public record CrcFailureViolation(int producerId, long sequence, String topic, int partition,
                                      long timestampMs) implements Violation {}

    public record LostMessageRange(int producerId, String topic, long fromSequence, long toSequence) {
        public long count() {
            return toSequence - fromSequence + 1;
        }
    }

    public record LossReport(long totalLost, List<LostMessageRange> ranges) {}

    public ModelChecker(boolean detectDuplicates, boolean detectOutOfOrder, int logDuplicateCount) {
        this.detectDuplicates = detectDuplicates;
        this.detectOutOfOrder = detectOutOfOrder;
        this.logDuplicateCount = logDuplicateCount;
    }

    public ModelChecker(boolean detectDuplicates, boolean detectOutOfOrder) {
        this(detectDuplicates, detectOutOfOrder, 100);
    }

    /**
     * Called on producer ack callback (not at send time).
     * Records that a message was successfully sent.
     */
    public void messageSent(int producerId, long sequence, String topic) {
        totalSent.increment();
        producerTopics.putIfAbsent(producerId, topic);
        sentSequences.computeIfAbsent(producerId, k -> new AtomicLong(-1))
                .accumulateAndGet(sequence, Math::max);
    }

    /**
     * Called from consumer thread when a message is received.
     * Checks for duplicates, out-of-order, and CRC failures.
     * Does NOT check against the sent set (consumer-before-ack race).
     */
    public void messageReceived(int producerId, long sequence, String topic, int partition,
                                boolean crcValid) {
        totalReceived.increment();

        // CRC check
        if (!crcValid) {
            crcFailureCount.increment();
            addViolation(new CrcFailureViolation(producerId, sequence, topic, partition,
                    System.currentTimeMillis()));
        }

        // Duplicate check
        boolean isDuplicate = false;
        if (detectDuplicates) {
            SegmentedBitSet received = receivedSequences.computeIfAbsent(producerId,
                    k -> new SegmentedBitSet());
            synchronized (received) {
                isDuplicate = received.get(sequence);
                received.set(sequence);
            }
            if (isDuplicate) {
                long count = duplicateCount.sum() + 1;
                duplicateCount.increment();
                long highSeq = producerHighSequence
                        .computeIfAbsent(producerId, k -> new AtomicLong(-1)).get();
                if (count <= logDuplicateCount) {
                    log.warn("DUPLICATE: producerId={}, sequence={}, topic={}, partition={}, highSequence={} (#{} of first {})",
                            producerId, sequence, topic, partition, highSeq, count, logDuplicateCount);
                }
                addViolation(new DuplicateViolation(producerId, sequence, topic, partition,
                        highSeq, System.currentTimeMillis()));
            }
        } else {
            // Still track received sequences for loss detection, even if not checking duplicates
            SegmentedBitSet received = receivedSequences.computeIfAbsent(producerId,
                    k -> new SegmentedBitSet());
            synchronized (received) {
                isDuplicate = received.get(sequence);
                received.set(sequence);
            }
        }

        // Track high sequence for all non-duplicate messages (used by duplicate
        // violation reporting and out-of-order detection)
        if (!isDuplicate) {
            AtomicLong highSeq = producerHighSequence.computeIfAbsent(producerId,
                    k -> new AtomicLong(-1));
            long prev = highSeq.get();
            if (detectOutOfOrder && sequence < prev) {
                outOfOrderCount.increment();
                addViolation(new OutOfOrderViolation(producerId, sequence, prev, topic, partition,
                        System.currentTimeMillis()));
            }
            highSeq.accumulateAndGet(sequence, Math::max);
        }
    }

    /**
     * Compute data loss after the grace period. Compares sent ranges against
     * received sequences to find gaps.
     *
     * Must be called after all producer ack callbacks have completed.
     */
    public LossReport computeLoss() {
        List<LostMessageRange> allRanges = new ArrayList<>();
        long totalLost = 0;

        for (var entry : sentSequences.entrySet()) {
            int producerId = entry.getKey();
            long maxSent = entry.getValue().get();
            if (maxSent < 0) continue;

            String topic = producerTopics.getOrDefault(producerId, "");

            SegmentedBitSet received = receivedSequences.get(producerId);
            if (received == null) {
                // No messages received at all for this producer
                allRanges.add(new LostMessageRange(producerId, topic, 0, maxSent));
                totalLost += maxSent + 1;
                continue;
            }

            synchronized (received) {
                List<long[]> gaps = received.findGaps(maxSent);
                for (long[] gap : gaps) {
                    allRanges.add(new LostMessageRange(producerId, topic, gap[0], gap[1]));
                    totalLost += gap[1] - gap[0] + 1;
                }
            }
        }

        return new LossReport(totalLost, allRanges);
    }

    /**
     * Reset all state.
     */
    public void reset() {
        sentSequences.clear();
        producerTopics.clear();
        receivedSequences.clear();
        producerHighSequence.clear();
        duplicateCount.reset();
        outOfOrderCount.reset();
        crcFailureCount.reset();
        totalSent.reset();
        totalReceived.reset();
        violations.clear();
    }

    public long getDuplicateCount() {
        return duplicateCount.sum();
    }

    public long getOutOfOrderCount() {
        return outOfOrderCount.sum();
    }

    public long getCrcFailureCount() {
        return crcFailureCount.sum();
    }

    public long getTotalSent() {
        return totalSent.sum();
    }

    public long getTotalReceived() {
        return totalReceived.sum();
    }

    public List<Violation> getViolations() {
        return List.copyOf(violations);
    }

    private void addViolation(Violation violation) {
        if (violations.size() < MAX_VIOLATIONS) {
            violations.add(violation);
        }
    }
}
