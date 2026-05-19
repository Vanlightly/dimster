package dev.dimster.engines.correctness;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class ModelCheckerTest {

    @Test
    public void noViolationsWhenAllReceivedInOrder() {
        ModelChecker checker = new ModelChecker(true, true);

        // Send and receive 100 messages in order
        for (long seq = 0; seq < 100; seq++) {
            checker.messageSent(0, seq, "test-topic");
            checker.messageReceived(0, seq, "test-topic", 0, true);
        }

        assertEquals(100, checker.getTotalSent());
        assertEquals(100, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());
        assertEquals(0, checker.getOutOfOrderCount());
        assertEquals(0, checker.getCrcFailureCount());
        assertTrue(checker.getViolations().isEmpty());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
        assertTrue(loss.ranges().isEmpty());
    }

    @Test
    public void detectsDuplicates() {
        ModelChecker checker = new ModelChecker(true, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageSent(0, 1, "test-topic");

        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 1, "test-topic", 0, true);
        checker.messageReceived(0, 0, "test-topic", 0, true); // duplicate

        assertEquals(3, checker.getTotalReceived());
        assertEquals(1, checker.getDuplicateCount());
        assertEquals(1, checker.getViolations().size());
        assertTrue(checker.getViolations().get(0) instanceof ModelChecker.DuplicateViolation);
    }

    @Test
    public void detectsOutOfOrder() {
        ModelChecker checker = new ModelChecker(true, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageSent(0, 1, "test-topic");
        checker.messageSent(0, 2, "test-topic");

        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 2, "test-topic", 0, true);
        checker.messageReceived(0, 1, "test-topic", 0, true); // out of order on partition 0

        assertEquals(1, checker.getOutOfOrderCount());
        assertEquals(1, checker.getViolations().size());

        var v = (ModelChecker.OutOfOrderViolation) checker.getViolations().get(0);
        assertEquals(0, v.producerId());
        assertEquals(1, v.sequence());
        assertEquals(2, v.highSequence());
    }

    @Test
    public void outOfOrderDetectedAcrossPartitions() {
        ModelChecker checker = new ModelChecker(true, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageSent(0, 1, "test-topic");

        // Out-of-order is per-producer stream, regardless of partition.
        // Receiving seq 1 before seq 0 is out-of-order even on different partitions.
        checker.messageReceived(0, 1, "test-topic", 0, true);
        checker.messageReceived(0, 0, "test-topic", 1, true);

        assertEquals(1, checker.getOutOfOrderCount());
    }

    @Test
    public void detectsCrcFailure() {
        ModelChecker checker = new ModelChecker(true, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageReceived(0, 0, "test-topic", 0, false); // CRC failure

        assertEquals(1, checker.getCrcFailureCount());
        assertEquals(1, checker.getViolations().size());
        assertTrue(checker.getViolations().get(0) instanceof ModelChecker.CrcFailureViolation);
    }

    @Test
    public void detectsDataLoss() {
        ModelChecker checker = new ModelChecker(true, true);

        // Send 10 messages
        for (long seq = 0; seq < 10; seq++) {
            checker.messageSent(0, seq, "test-topic");
        }

        // Receive all except seq 3 and 7
        for (long seq = 0; seq < 10; seq++) {
            if (seq != 3 && seq != 7) {
                checker.messageReceived(0, seq, "test-topic", 0, true);
            }
        }

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(2, loss.totalLost());
        assertEquals(2, loss.ranges().size());

        // seq 3 lost
        assertEquals(3, loss.ranges().get(0).fromSequence());
        assertEquals(3, loss.ranges().get(0).toSequence());
        assertEquals(1, loss.ranges().get(0).count());

        // seq 7 lost
        assertEquals(7, loss.ranges().get(1).fromSequence());
        assertEquals(7, loss.ranges().get(1).toSequence());
    }

    @Test
    public void detectsContiguousLossRange() {
        ModelChecker checker = new ModelChecker(true, true);

        for (long seq = 0; seq < 10; seq++) {
            checker.messageSent(0, seq, "test-topic");
        }

        // Only receive first 3 and last 3
        for (long seq = 0; seq < 3; seq++) {
            checker.messageReceived(0, seq, "test-topic", 0, true);
        }
        for (long seq = 7; seq < 10; seq++) {
            checker.messageReceived(0, seq, "test-topic", 0, true);
        }

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(4, loss.totalLost());
        assertEquals(1, loss.ranges().size());
        assertEquals(3, loss.ranges().get(0).fromSequence());
        assertEquals(6, loss.ranges().get(0).toSequence());
        assertEquals(4, loss.ranges().get(0).count());
    }

    @Test
    public void multipleProducers() {
        ModelChecker checker = new ModelChecker(true, true);

        // Producer 0: send 5, receive all
        for (long seq = 0; seq < 5; seq++) {
            checker.messageSent(0, seq, "test-topic");
            checker.messageReceived(0, seq, "test-topic", 0, true);
        }

        // Producer 1: send 5, lose seq 2
        for (long seq = 0; seq < 5; seq++) {
            checker.messageSent(1, seq, "test-topic");
            if (seq != 2) {
                checker.messageReceived(1, seq, "test-topic", 1, true);
            }
        }

        assertEquals(10, checker.getTotalSent());
        assertEquals(9, checker.getTotalReceived());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(1, loss.totalLost());
        assertEquals(1, loss.ranges().size());
        assertEquals(1, loss.ranges().get(0).producerId());
        assertEquals(2, loss.ranges().get(0).fromSequence());
    }

    @Test
    public void consumerBeforeAckIsNotAViolation() {
        ModelChecker checker = new ModelChecker(true, true);

        // Consumer receives seq 0 BEFORE producer gets ack
        checker.messageReceived(0, 0, "test-topic", 0, true);

        // No violations should be recorded (consumer-side only checks don't reference sent set)
        assertEquals(0, checker.getDuplicateCount());
        assertEquals(0, checker.getOutOfOrderCount());
        assertEquals(0, checker.getCrcFailureCount());

        // Now producer ack arrives
        checker.messageSent(0, 0, "test-topic");

        // Loss check should show no loss
        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
        assertTrue(loss.ranges().isEmpty());
    }

    @Test
    public void consumerBeforeAckWithMultipleMessages() {
        ModelChecker checker = new ModelChecker(true, true);

        // Simulate: consumer receives messages before some acks arrive
        // This can happen due to Kafka's async pipeline
        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 1, "test-topic", 0, true);
        checker.messageSent(0, 0, "test-topic");
        checker.messageReceived(0, 2, "test-topic", 0, true);
        checker.messageSent(0, 1, "test-topic");
        checker.messageSent(0, 2, "test-topic");

        assertEquals(3, checker.getTotalSent());
        assertEquals(3, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());
        assertEquals(0, checker.getOutOfOrderCount());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    @Test
    public void resetClearsAllState() {
        ModelChecker checker = new ModelChecker(true, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 0, "test-topic", 0, true); // duplicate

        assertEquals(1, checker.getDuplicateCount());

        checker.reset();

        assertEquals(0, checker.getTotalSent());
        assertEquals(0, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());
        assertTrue(checker.getViolations().isEmpty());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    @Test
    public void disabledDuplicateDetection() {
        ModelChecker checker = new ModelChecker(false, true);

        checker.messageSent(0, 0, "test-topic");
        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 0, "test-topic", 0, true); // duplicate not detected

        assertEquals(0, checker.getDuplicateCount());

        // But loss detection should still work
        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    @Test
    public void disabledOutOfOrderDetection() {
        ModelChecker checker = new ModelChecker(true, false);

        checker.messageSent(0, 0, "test-topic");
        checker.messageSent(0, 1, "test-topic");
        checker.messageSent(0, 2, "test-topic");

        checker.messageReceived(0, 2, "test-topic", 0, true);
        checker.messageReceived(0, 0, "test-topic", 0, true);
        checker.messageReceived(0, 1, "test-topic", 0, true);

        assertEquals(0, checker.getOutOfOrderCount());
    }

    @Test
    public void concurrentAccess() throws Exception {
        ModelChecker checker = new ModelChecker(true, true);
        int numProducers = 4;
        int messagesPerProducer = 10_000;
        CountDownLatch latch = new CountDownLatch(numProducers * 2); // producers + consumers

        ExecutorService executor = Executors.newFixedThreadPool(numProducers * 2);

        // Start producer threads
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            executor.submit(() -> {
                for (long seq = 0; seq < messagesPerProducer; seq++) {
                    checker.messageSent(producerId, seq, "test-topic");
                }
                latch.countDown();
            });
        }

        // Start consumer threads
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            executor.submit(() -> {
                for (long seq = 0; seq < messagesPerProducer; seq++) {
                    checker.messageReceived(producerId, seq, "test-topic", producerId % 3, true);
                }
                latch.countDown();
            });
        }

        assertTrue("Threads should complete within 10s",
                latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numProducers * messagesPerProducer, checker.getTotalSent());
        assertEquals(numProducers * messagesPerProducer, checker.getTotalReceived());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    @Test
    public void noMessagesReceivedForProducer() {
        ModelChecker checker = new ModelChecker(true, true);

        // Producer sent messages but none received
        for (long seq = 0; seq < 5; seq++) {
            checker.messageSent(0, seq, "test-topic");
        }

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(5, loss.totalLost());
        assertEquals(1, loss.ranges().size());
        assertEquals(0, loss.ranges().get(0).fromSequence());
        assertEquals(4, loss.ranges().get(0).toSequence());
    }

    @Test
    public void violationsCapped() {
        ModelChecker checker = new ModelChecker(true, true, 0);

        // Generate more than MAX_VIOLATIONS (1000) duplicates
        checker.messageSent(0, 0, "test-topic");
        checker.messageReceived(0, 0, "test-topic", 0, true);
        for (int i = 0; i < 1500; i++) {
            checker.messageReceived(0, 0, "test-topic", 0, true);
        }

        assertEquals(1500, checker.getDuplicateCount());
        // Violations list should be capped
        assertTrue(checker.getViolations().size() <= 1000);
    }
}
