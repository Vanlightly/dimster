package dev.dimster.availability;

import static org.junit.Assert.*;

import org.junit.Test;

public class AvailabilityRecorderTest {

    /** Helper: register + complete a probe in one step (normal test flow). */
    private void probe(AvailabilityRecorder recorder, int partition, long sequence,
                       long timestampMs, long durationMs) {
        recorder.probeStarted(partition, sequence, timestampMs);
        recorder.probeProduced(partition, sequence, timestampMs, durationMs);
    }

    /** Helper: register + fail a probe in one step. */
    private void probeFail(AvailabilityRecorder recorder, int partition, long sequence,
                           long timestampMs, long durationMs, String error) {
        recorder.probeStarted(partition, sequence, timestampMs);
        recorder.probeFailed(partition, sequence, timestampMs, durationMs, error);
    }

    @Test
    public void allProbesWithinThreshold() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long baseTime = 1000000;
        for (int i = 0; i < 10; i++) {
            long ts = baseTime + i * 1000;
            probe(recorder, 0, i, ts, 50); // 50ms produce time
            recorder.probeReceived(0, i, ts, ts + 100); // 100ms e2e
        }

        long nowMs = baseTime + 9 * 1000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        assertEquals(100.0, result.overallAvailabilityPct, 0.01);
        assertEquals(0, result.totalUnavailableMs);

        assertEquals(1, result.partitionResults.size());
        var pr = result.partitionResults.get(0);
        assertEquals(10, pr.probesSent);
        assertEquals(10, pr.probesReceived);
        assertEquals(0, pr.probesFailed);
        assertEquals(100.0, pr.producerAvailabilityPct, 0.01);
        assertEquals(100.0, pr.e2eAvailabilityPct, 0.01);
        assertTrue(pr.unavailabilityEvents.isEmpty());
    }

    @Test
    public void producerUnavailabilityDetected() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long baseTime = 1000000;

        // 5 normal probes (50ms each)
        probe(recorder, 0, 1, baseTime, 50);
        probe(recorder, 0, 2, baseTime + 1000, 50);
        probe(recorder, 0, 3, baseTime + 2000, 50);
        probe(recorder, 0, 4, baseTime + 3000, 50);
        probe(recorder, 0, 5, baseTime + 4000, 50);

        // 1 slow probe (8 seconds) — this is 8000ms of unavailability
        probe(recorder, 0, 6, baseTime + 5000, 8000);

        // 4 more normal probes
        probe(recorder, 0, 7, baseTime + 13000, 50);
        probe(recorder, 0, 8, baseTime + 14000, 50);
        probe(recorder, 0, 9, baseTime + 15000, 50);
        probe(recorder, 0, 10, baseTime + 16000, 50);

        long nowMs = baseTime + 16000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);
        assertEquals(10, pr.probesSent);
        assertEquals(8000, pr.producerUnavailableMs);
        assertTrue(pr.producerAvailabilityPct < 100.0);

        // Availability pct = (elapsed - unavailability) / elapsed
        long elapsed = nowMs - baseTime;
        int expectedPct = (int)(100.0 * ((elapsed - 8000.0) / elapsed));
        int actualPct = (int)pr.producerAvailabilityPct;
        assertEquals(expectedPct, actualPct);

        // Should have one producer unavailability event
        long producerEvents = pr.unavailabilityEvents.stream()
                .filter(e -> "PRODUCER".equals(e.type))
                .count();
        assertEquals(1, producerEvents);
    }

    @Test
    public void coordinatedOmissionHandled() {
        // A 30-second blocking probe should count as 30s of unavailability,
        // not "1 probe out of N" (which would be coordinated omission)
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long baseTime = 1000000;

        // 1 normal probe
        probe(recorder, 0, 0, baseTime, 50);

        // 1 probe that blocks for 30 seconds (simulating leader failover)
        probe(recorder, 0, 1, baseTime + 1000, 30000);

        // 1 normal probe after recovery
        probe(recorder, 0, 2, baseTime + 32000, 50);

        long nowMs = baseTime + 32000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Elapsed = nowMs - baseTime = 32050ms
        assertEquals(32050, pr.elapsedMs);
        // Unavailable = 30000ms (the entire slow probe duration)
        assertEquals(30000, pr.producerUnavailableMs);
        // Availability ≈ (32050 - 30000) / 32050 ≈ 6.4%
        // NOT 66.7% (2 out of 3 probes OK) — that would be coordinated omission
        double expectedPct = 100.0 * (32050 - 30000) / 32050;
        assertEquals(expectedPct, pr.producerAvailabilityPct, 0.1);
        assertTrue(pr.producerAvailabilityPct < 10.0);
    }

    @Test
    public void failedProbesCountAsUnavailable() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        probe(recorder, 0, 0, 1000, 50);
        // Failed probe took 30s to timeout
        probeFail(recorder, 0, 1, 2000, 30000, "TimeoutException");
        probe(recorder, 0, 2, 33000, 50);

        long nowMs = 33000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);
        assertEquals(3, pr.probesSent);
        assertEquals(1, pr.probesFailed);
        assertEquals(30000, pr.producerUnavailableMs);
    }

    @Test
    public void e2eUnavailabilityDetected() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long baseTime = 1000000;

        // Normal probe
        probe(recorder, 0, 0, baseTime, 50);
        recorder.probeReceived(0, 0, baseTime, baseTime + 100);

        // Slow e2e (producer was fast but consumer was slow — 7s e2e)
        probe(recorder, 0, 1, baseTime + 1000, 50);
        recorder.probeReceived(0, 1, baseTime + 1000, baseTime + 1000 + 7000);

        // Normal again
        probe(recorder, 0, 2, baseTime + 2000, 50);
        recorder.probeReceived(0, 2, baseTime + 2000, baseTime + 2100);

        long nowMs = baseTime + 2000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);
        assertEquals(7000, pr.e2eUnavailableMs);

        long e2eEvents = pr.unavailabilityEvents.stream()
                .filter(e -> "E2E".equals(e.type))
                .count();
        assertEquals(1, e2eEvents);
    }

    @Test
    public void multiplePartitionsTrackedIndependently() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        // Partition 0: all good
        for (int i = 0; i < 5; i++) {
            probe(recorder, 0, i, i * 1000, 50);
            recorder.probeReceived(0, i, i * 1000, i * 1000 + 100);
        }

        // Partition 1: has a slow probe
        for (int i = 0; i < 5; i++) {
            long duration = (i == 2) ? 8000 : 50;
            probe(recorder, 1, i, i * 1000, duration);
        }

        // P1 has a slow probe at i=2: ts=2000, duration=8000, ends at 10000
        long nowMs = 10000; // covers both partitions' activity
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        assertEquals(2, result.partitionResults.size());

        var p0 = result.partitionResults.stream()
                .filter(p -> p.partition == 0).findFirst().orElseThrow();
        var p1 = result.partitionResults.stream()
                .filter(p -> p.partition == 1).findFirst().orElseThrow();

        assertEquals(100.0, p0.producerAvailabilityPct, 0.01);
        assertTrue(p1.producerAvailabilityPct < 100.0);
    }

    @Test
    public void timeSeriesPopulated() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        probe(recorder, 0, 0, 1000, 50);
        recorder.probeReceived(0, 0, 1000, 1100);
        probe(recorder, 0, 1, 2000, 60);
        recorder.probeReceived(0, 1, 2000, 2120);

        long nowMs = 2000 + 60; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);
        assertEquals(2, pr.timeSeries.size());
        assertEquals(50, pr.timeSeries.get(0).produceDurationMs);
        assertEquals(100, pr.timeSeries.get(0).e2eLatencyMs);
        assertEquals(60, pr.timeSeries.get(1).produceDurationMs);
        assertEquals(120, pr.timeSeries.get(1).e2eLatencyMs);
    }

    @Test
    public void e2eOverlappingWindowsMergedCorrectly() {
        // Simulates a 35s Kafka outage with consumer catch-up afterward.
        // After recovery, successive probes are fast to produce but the consumer
        // is behind, so e2e latencies are high and their windows overlap.
        // Without merging, the e2e unavailability would be the SUM of all latencies.
        // With merging, it's the wall-clock duration of the single event.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 1000000;

        // Normal probe before outage
        probe(recorder, 0, 0, base, 50);
        recorder.probeReceived(0, 0, base, base + 100);

        // Probe during outage: blocks for 35s on producer side
        probe(recorder, 0, 1, base + 1000, 35000);
        // Consumer receives it at base + 40000 (e2e = 39000ms)
        recorder.probeReceived(0, 1, base + 1000, base + 40000);

        // Post-recovery probes: fast to produce but consumer is behind
        probe(recorder, 0, 2, base + 37000, 50);
        recorder.probeReceived(0, 2, base + 37000, base + 42000); // e2e = 5000 (at threshold, not over)

        probe(recorder, 0, 3, base + 38000, 50);
        recorder.probeReceived(0, 3, base + 38000, base + 41000); // e2e = 3000 (below threshold)

        // Normal probe
        probe(recorder, 0, 4, base + 39000, 50);
        recorder.probeReceived(0, 4, base + 39000, base + 39100); // e2e = 100ms

        long nowMs = base + 39000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Producer unavailability: one event of 35s
        assertEquals(35000, pr.producerUnavailableMs);

        // E2E unavailability: one merged event [base+1000 .. base+40000] = 39000ms
        assertEquals(39000, pr.e2eUnavailableMs);

        // Verify there's exactly one e2e event
        long e2eEvents = pr.unavailabilityEvents.stream()
                .filter(e -> "E2E".equals(e.type))
                .count();
        assertEquals(1, e2eEvents);

        // Overall availability is determined by the worse metric (e2e)
        assertTrue(result.overallAvailabilityPct < 1.0);
        assertTrue(result.overallAvailabilityPct >= 0.0);
    }

    @Test
    public void consumerMatchesBeforeProducerCompletes() {
        // Simulates the race condition: consumer receives probe before
        // probeProduced() is called. Using probeStarted() ensures the
        // record exists for matching.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 1000000;

        // Register probe before sending
        recorder.probeStarted(0, 0, base);
        // Consumer receives it before producer's send() returns
        recorder.probeReceived(0, 0, base, base + 80);
        // Producer's send() completes
        recorder.probeProduced(0, 0, base, 50);

        long nowMs = base + 50; // just after probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        assertEquals(1, pr.probesSent);
        assertEquals(1, pr.probesReceived);
        assertEquals(50, pr.timeSeries.get(0).produceDurationMs);
        assertEquals(80, pr.timeSeries.get(0).e2eLatencyMs);
        assertEquals(100.0, pr.producerAvailabilityPct, 0.01);
        assertEquals(100.0, pr.e2eAvailabilityPct, 0.01);
    }

    @Test
    public void overallMergesProducerAndE2eWindows() {
        // Producer unavailable at T=10-20, e2e unavailable at T=15-30.
        // Overall should be the union: T=10-30 = 20s, not max(10,15) = 15s.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 0;

        // Normal probe
        probe(recorder, 0, 0, base, 50);
        recorder.probeReceived(0, 0, base, base + 100);

        // Slow producer at T=10000 (10s produce, 100ms e2e after that)
        probe(recorder, 0, 1, base + 10000, 10000);
        recorder.probeReceived(0, 1, base + 10000, base + 10100);

        // Normal producer at T=21000, but slow e2e (consumer behind, 15s e2e)
        probe(recorder, 0, 2, base + 21000, 50);
        recorder.probeReceived(0, 2, base + 21000, base + 21000 + 15000);

        // Normal probe
        probe(recorder, 0, 3, base + 37000, 50);
        recorder.probeReceived(0, 3, base + 37000, base + 37100);

        long nowMs = 37000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Producer event: [10000, 20000] = 10000ms
        assertEquals(10000, pr.producerUnavailableMs);
        // E2E event: [21000, 36000] = 15000ms
        assertEquals(15000, pr.e2eUnavailableMs);

        // Overall: two non-overlapping events [10000,20000] + [21000,36000] = 25000ms
        assertEquals(25000, result.totalUnavailableMs);

        // elapsed = nowMs - 0 = 37050
        // overall avail = (37050 - 25000) / 37050 ≈ 32.5%
        double expectedPct = 100.0 * (37050 - 25000) / 37050.0;
        assertEquals(expectedPct, result.overallAvailabilityPct, 0.1);
    }

    @Test
    public void overallMergesOverlappingProducerAndE2eWindows() {
        // Producer unavailable at T=10-25, e2e unavailable at T=20-35.
        // Overlap at T=20-25. Overall = union = T=10-35 = 25s.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 0;

        // Normal probe
        probe(recorder, 0, 0, base, 50);
        recorder.probeReceived(0, 0, base, base + 100);

        // Slow producer at T=10000 (15s produce), fast e2e after
        probe(recorder, 0, 1, base + 10000, 15000);
        recorder.probeReceived(0, 1, base + 10000, base + 10100);

        // Normal producer at T=26000, but slow e2e (15s)
        probe(recorder, 0, 2, base + 26000, 50);
        recorder.probeReceived(0, 2, base + 26000, base + 26000 + 15000);

        // Normal probe
        probe(recorder, 0, 3, base + 42000, 50);
        recorder.probeReceived(0, 3, base + 42000, base + 42100);

        long nowMs = 42000 + 50; // just after last probe completes
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Producer event: [10000, 25000] = 15000ms
        assertEquals(15000, pr.producerUnavailableMs);
        // E2E event: [26000, 41000] = 15000ms
        assertEquals(15000, pr.e2eUnavailableMs);

        // Overall: non-overlapping [10000,25000] + [26000,41000] = 30000ms
        assertEquals(30000, result.totalUnavailableMs);
    }

    @Test
    public void inFlightProbesResolvedAtComputeTime() {
        // Simulates test ending while a probe is still blocking (Kafka down).
        // The in-flight probe should count as unavailable for both producer and e2e.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 1000000;

        // Normal probes (produced and received)
        probe(recorder, 0, 0, base, 50);
        recorder.probeReceived(0, 0, base, base + 100);
        probe(recorder, 0, 1, base + 1000, 50);
        recorder.probeReceived(0, 1, base + 1000, base + 1100);

        // Probe started but never completed (Kafka went down, send is blocking)
        recorder.probeStarted(0, 2, base + 2000);
        // No probeProduced or probeFailed call — still in-flight
        // No probeReceived either — consumer never got it

        // Compute result 20 seconds after the in-flight probe started
        long nowMs = base + 22000;
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Elapsed = nowMs - base = 22000ms (wall-clock, not frozen at last completed probe)
        assertEquals(22000, pr.elapsedMs);

        // The in-flight probe has effective duration = 22000 - 2000 = 20000ms > threshold
        // So it should be detected as producer unavailability
        assertTrue(pr.producerUnavailableMs > 0);
        assertTrue(pr.producerAvailabilityPct < 100.0);

        // The unreceived probe also has effective e2e latency = 20000ms > threshold
        // So it should be detected as e2e unavailability too
        assertTrue(pr.e2eUnavailableMs > 0);
        assertTrue(pr.e2eAvailabilityPct < 100.0);

        // Should have one producer and one e2e unavailability event
        long producerEvents = pr.unavailabilityEvents.stream()
                .filter(e -> "PRODUCER".equals(e.type))
                .count();
        assertEquals(1, producerEvents);

        long e2eEvents = pr.unavailabilityEvents.stream()
                .filter(e -> "E2E".equals(e.type))
                .count();
        assertEquals(1, e2eEvents);
    }

    @Test
    public void producedButUnreceivedProbesCountAsE2eUnavailable() {
        // Probe was produced successfully but never consumed (consumer down).
        // Should count as e2e unavailability but NOT producer unavailability.
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        long base = 1000000;

        // Normal probe (produced and received)
        probe(recorder, 0, 0, base, 50);
        recorder.probeReceived(0, 0, base, base + 100);

        // Probe produced successfully but never received by consumer
        probe(recorder, 0, 1, base + 1000, 50);
        // No probeReceived call

        // Compute result 20 seconds later
        long nowMs = base + 21000;
        AvailabilityResult.AvailabilityData result = recorder.computeResult(nowMs);
        var pr = result.partitionResults.get(0);

        // Producer was fine — fast produce
        assertEquals(0, pr.producerUnavailableMs);
        assertEquals(100.0, pr.producerAvailabilityPct, 0.01);

        // But e2e is unavailable — probe never consumed
        // Effective e2e latency = 21000 - 1000 = 20000ms > threshold
        assertTrue(pr.e2eUnavailableMs > 0);
        assertTrue(pr.e2eAvailabilityPct < 100.0);
    }

    @Test
    public void currentStatusStringFormat() {
        AvailabilityRecorder recorder = new AvailabilityRecorder(5000);

        probe(recorder, 0, 0, 1000, 50);
        recorder.probeReceived(0, 0, 1000, 1100);

        String status = recorder.currentStatus();
        assertTrue(status.contains("probes(sent=1"));
        assertTrue(status.contains("recv=1"));
        assertTrue(status.contains("unavail(producer="));
    }
}
