package dev.dimster.availability;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe per-partition availability tracker.
 *
 * Records probe produce durations and consumer-side receipt times,
 * then computes availability percentages and unavailability events.
 *
 * Uses time-based availability measurement to avoid coordinated omission:
 * when a probe blocks for 30s, that counts as 30s of unavailability,
 * not just "1 bad probe out of N". The denominator is actual wall-clock
 * elapsed time per partition.
 */
public class AvailabilityRecorder {

    private static final Logger log = LoggerFactory.getLogger(AvailabilityRecorder.class);

    private final long unavailabilityThresholdMs;
    private final ConcurrentHashMap<Integer, PartitionData> partitions = new ConcurrentHashMap<>();

    public AvailabilityRecorder(long unavailabilityThresholdMs) {
        this.unavailabilityThresholdMs = unavailabilityThresholdMs;
    }

    // -- Producer-side recording --

    /**
     * Pre-register a probe before sending. Creates the ProbeRecord so the consumer
     * can match it immediately if the message arrives before send() returns.
     */
    public void probeStarted(int partition, long sequence, long produceTimestampMs) {
        PartitionData data = partitions.computeIfAbsent(partition, k -> new PartitionData());
        data.initStartTime(produceTimestampMs);
        data.totalProbesSent.increment();

        synchronized (data.probeRecords) {
            data.probeRecords.add(new ProbeRecord(sequence, produceTimestampMs, -1, -1, -1));
        }
    }

    /**
     * Record the outcome of a completed probe send. Updates the pre-registered record.
     */
    public void probeProduced(int partition, long sequence, long produceTimestampMs, long produceDurationMs) {
        PartitionData data = partitions.get(partition);
        if (data == null) return;

        synchronized (data.probeRecords) {
            for (int i = data.probeRecords.size() - 1; i >= 0; i--) {
                ProbeRecord r = data.probeRecords.get(i);
                if (r.sequence == sequence) {
                    data.probeRecords.set(i, new ProbeRecord(
                            r.sequence, r.produceTimestampMs, produceDurationMs,
                            r.receiveTimestampMs, r.e2eLatencyMs));
                    break;
                }
            }
        }

        if (produceDurationMs > unavailabilityThresholdMs) {
            data.producerUnavailableMs.add(produceDurationMs);
        }
    }

    public void probeFailed(int partition, long sequence, long produceTimestampMs,
                            long failureDurationMs, String error) {
        PartitionData data = partitions.get(partition);
        if (data == null) return;

        data.totalProbesFailed.increment();
        data.producerUnavailableMs.add(failureDurationMs);

        synchronized (data.probeRecords) {
            for (int i = data.probeRecords.size() - 1; i >= 0; i--) {
                ProbeRecord r = data.probeRecords.get(i);
                if (r.sequence == sequence) {
                    data.probeRecords.set(i, new ProbeRecord(
                            r.sequence, r.produceTimestampMs, failureDurationMs,
                            r.receiveTimestampMs, r.e2eLatencyMs));
                    break;
                }
            }
        }

        log.warn("Availability probe failed for partition {}: {}", partition, error);
    }

    // -- Consumer-side recording --

    public void probeReceived(int partition, long sequence, long produceTimestampMs, long receiveTimestampMs) {
        PartitionData data = partitions.get(partition);
        if (data == null) return;
        data.totalProbesReceived.increment();

        long e2eLatencyMs = receiveTimestampMs - produceTimestampMs;

        // Update the matching probe record if it exists
        synchronized (data.probeRecords) {
            for (int i = data.probeRecords.size() - 1; i >= 0; i--) {
                ProbeRecord r = data.probeRecords.get(i);
                if (r.sequence == sequence) {
                    data.probeRecords.set(i, new ProbeRecord(
                            r.sequence, r.produceTimestampMs, r.produceDurationMs,
                            receiveTimestampMs, e2eLatencyMs));
                    break;
                }
            }
        }

        if (e2eLatencyMs > unavailabilityThresholdMs) {
            data.e2eUnavailableMs.add(e2eLatencyMs);
        }
    }

    // -- Result computation --

    public AvailabilityResult.AvailabilityData computeResult(long nowMs) {
        var result = new AvailabilityResult.AvailabilityData();

        long totalElapsedMs = 0;
        long maxElapsedMs = 0;
        long totalProducerUnavailMs = 0;
        long totalE2eUnavailMs = 0;

        for (var entry : partitions.entrySet()) {
            int partition = entry.getKey();
            PartitionData data = entry.getValue();

            var pr = new AvailabilityResult.PartitionResult();
            pr.partition = partition;
            pr.probesSent = data.totalProbesSent.sum();
            pr.probesReceived = data.totalProbesReceived.sum();
            pr.probesFailed = data.totalProbesFailed.sum();

            // Copy probe records and resolve in-flight probes
            List<ProbeRecord> records;
            synchronized (data.probeRecords) {
                records = new ArrayList<>(data.probeRecords);
            }

            // Resolve unfinished probes: treat as having blocked until now.
            // A probe with produceDurationMs < 0 is still in-flight on the producer.
            // A probe with e2eLatencyMs < 0 was never received by the consumer.
            // Both cases mean the system is unavailable for the duration since the probe started.
            for (int i = 0; i < records.size(); i++) {
                ProbeRecord r = records.get(i);
                long produceDuration = r.produceDurationMs;
                long e2eLatency = r.e2eLatencyMs;
                boolean modified = false;

                if (produceDuration < 0) {
                    produceDuration = nowMs - r.produceTimestampMs;
                    modified = true;
                }
                if (e2eLatency < 0) {
                    e2eLatency = nowMs - r.produceTimestampMs;
                    modified = true;
                }

                if (modified) {
                    records.set(i, new ProbeRecord(
                            r.sequence, r.produceTimestampMs, produceDuration,
                            r.receiveTimestampMs, e2eLatency));
                }
            }

            // Wall-clock elapsed time since first probe
            long elapsedMs = data.startTimeMs >= 0 ? Math.max(0, nowMs - data.startTimeMs) : 0;
            pr.elapsedMs = elapsedMs;

            // Build time series
            for (ProbeRecord r : records) {
                var dp = new AvailabilityResult.ProbeDataPoint();
                dp.timestampMs = r.produceTimestampMs;
                dp.partition = partition;
                dp.produceDurationMs = r.produceDurationMs;
                dp.e2eLatencyMs = r.e2eLatencyMs;
                pr.timeSeries.add(dp);
            }

            // Compute unavailability from merged event windows to avoid
            // double-counting overlapping e2e latency periods
            pr.unavailabilityEvents = detectEvents(partition, records);
            for (var event : pr.unavailabilityEvents) {
                if ("PRODUCER".equals(event.type)) {
                    pr.producerUnavailableMs += event.durationMs;
                } else if ("E2E".equals(event.type)) {
                    pr.e2eUnavailableMs += event.durationMs;
                }
            }

            // Availability = (elapsed - unavailable) / elapsed
            if (elapsedMs > 0) {
                pr.producerAvailabilityPct =
                        100.0 * Math.max(0, elapsedMs - pr.producerUnavailableMs) / elapsedMs;
                pr.e2eAvailabilityPct =
                        100.0 * Math.max(0, elapsedMs - pr.e2eUnavailableMs) / elapsedMs;
            }

            result.partitionResults.add(pr);
            totalElapsedMs += elapsedMs;
            maxElapsedMs = Math.max(maxElapsedMs, elapsedMs);
            totalProducerUnavailMs += pr.producerUnavailableMs;
            totalE2eUnavailMs += pr.e2eUnavailableMs;
        }

        // Aggregate stats
        result.partitions = partitions.size();
        result.wallClockMs = maxElapsedMs;
        result.totalElapsedMs = totalElapsedMs;
        result.producerUnavailableMs = totalProducerUnavailMs;
        result.e2eUnavailableMs = totalE2eUnavailMs;
        if (totalElapsedMs > 0) {
            result.producerAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalProducerUnavailMs) / totalElapsedMs;
            result.e2eAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalE2eUnavailMs) / totalElapsedMs;
        }

        // Overall availability: merge producer + e2e event windows per partition
        // so overlapping periods are counted once
        long totalUnavailMs = 0;
        for (var pr : result.partitionResults) {
            totalUnavailMs += mergedUnavailableDuration(pr.unavailabilityEvents);
        }
        result.totalUnavailableMs = totalUnavailMs;
        if (totalElapsedMs > 0) {
            result.overallAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalUnavailMs) / totalElapsedMs;
        }

        return result;
    }

    /**
     * Merge all unavailability events (regardless of type) into non-overlapping
     * intervals and return the total duration. This gives the wall-clock time
     * during which either producer or consumer (or both) were unavailable.
     */
    public static long mergedUnavailableDuration(List<AvailabilityResult.UnavailabilityEvent> events) {
        if (events.isEmpty()) return 0;

        // Sort by start time
        var sorted = events.stream()
                .sorted((a, b) -> Long.compare(a.startTimestampMs, b.startTimestampMs))
                .toList();

        long totalMs = 0;
        long mergedStart = sorted.get(0).startTimestampMs;
        long mergedEnd = sorted.get(0).endTimestampMs;

        for (int i = 1; i < sorted.size(); i++) {
            var e = sorted.get(i);
            if (e.startTimestampMs <= mergedEnd) {
                // Overlapping or adjacent — extend
                mergedEnd = Math.max(mergedEnd, e.endTimestampMs);
            } else {
                // Gap — flush current interval
                totalMs += mergedEnd - mergedStart;
                mergedStart = e.startTimestampMs;
                mergedEnd = e.endTimestampMs;
            }
        }
        totalMs += mergedEnd - mergedStart;

        return totalMs;
    }

    public String currentStatus() {
        long totalSent = 0;
        long totalReceived = 0;
        long totalFailed = 0;
        long totalProducerUnavailMs = 0;
        long totalE2eUnavailMs = 0;

        for (var data : partitions.values()) {
            totalSent += data.totalProbesSent.sum();
            totalReceived += data.totalProbesReceived.sum();
            totalFailed += data.totalProbesFailed.sum();
            totalProducerUnavailMs += data.producerUnavailableMs.sum();
            totalE2eUnavailMs += data.e2eUnavailableMs.sum();
        }

        return String.format("probes(sent=%d, recv=%d, fail=%d) unavail(producer=%dms, e2e=%dms)",
                totalSent, totalReceived, totalFailed, totalProducerUnavailMs, totalE2eUnavailMs);
    }

    private List<AvailabilityResult.UnavailabilityEvent> detectEvents(int partition, List<ProbeRecord> records) {
        List<AvailabilityResult.UnavailabilityEvent> events = new ArrayList<>();

        // Detect producer unavailability events (contiguous probes exceeding threshold)
        long eventStart = -1;
        long eventEnd = -1;

        for (ProbeRecord r : records) {
            if (r.produceDurationMs < 0) continue; // in-flight, not yet completed
            boolean unavailable = r.produceDurationMs > unavailabilityThresholdMs;

            if (unavailable) {
                if (eventStart < 0) {
                    eventStart = r.produceTimestampMs;
                }
                long duration = r.produceDurationMs > 0 ? r.produceDurationMs : unavailabilityThresholdMs;
                eventEnd = Math.max(eventEnd, r.produceTimestampMs + duration);
            } else {
                if (eventStart >= 0) {
                    var event = new AvailabilityResult.UnavailabilityEvent();
                    event.startTimestampMs = eventStart;
                    event.endTimestampMs = eventEnd;
                    event.durationMs = eventEnd - eventStart;
                    event.partition = partition;
                    event.type = "PRODUCER";
                    events.add(event);
                    eventStart = -1;
                    eventEnd = -1;
                }
            }
        }
        // Close any open event
        if (eventStart >= 0) {
            var event = new AvailabilityResult.UnavailabilityEvent();
            event.startTimestampMs = eventStart;
            event.endTimestampMs = eventEnd;
            event.durationMs = eventEnd - eventStart;
            event.partition = partition;
            event.type = "PRODUCER";
            events.add(event);
        }

        // Detect e2e unavailability events (using max to merge overlapping windows)
        eventStart = -1;
        eventEnd = -1;

        for (ProbeRecord r : records) {
            if (r.e2eLatencyMs < 0) continue; // not yet received by consumer
            boolean unavailable = r.e2eLatencyMs > unavailabilityThresholdMs;

            if (unavailable) {
                if (eventStart < 0) {
                    eventStart = r.produceTimestampMs;
                }
                long duration = r.e2eLatencyMs > 0 ? r.e2eLatencyMs : unavailabilityThresholdMs;
                eventEnd = Math.max(eventEnd, r.produceTimestampMs + duration);
            } else {
                if (eventStart >= 0) {
                    var event = new AvailabilityResult.UnavailabilityEvent();
                    event.startTimestampMs = eventStart;
                    event.endTimestampMs = eventEnd;
                    event.durationMs = eventEnd - eventStart;
                    event.partition = partition;
                    event.type = "E2E";
                    events.add(event);
                    eventStart = -1;
                    eventEnd = -1;
                }
            }
        }
        if (eventStart >= 0) {
            var event = new AvailabilityResult.UnavailabilityEvent();
            event.startTimestampMs = eventStart;
            event.endTimestampMs = eventEnd;
            event.durationMs = eventEnd - eventStart;
            event.partition = partition;
            event.type = "E2E";
            events.add(event);
        }

        return events;
    }

    // -- Internal types --

    private record ProbeRecord(long sequence, long produceTimestampMs, long produceDurationMs,
                               long receiveTimestampMs, long e2eLatencyMs) {}

    private static class PartitionData {
        volatile long startTimeMs = -1;
        final LongAdder totalProbesSent = new LongAdder();
        final LongAdder totalProbesReceived = new LongAdder();
        final LongAdder totalProbesFailed = new LongAdder();
        final LongAdder producerUnavailableMs = new LongAdder();
        final LongAdder e2eUnavailableMs = new LongAdder();
        final List<ProbeRecord> probeRecords = new ArrayList<>();

        void initStartTime(long timestampMs) {
            if (startTimeMs < 0) {
                startTimeMs = timestampMs;
            }
        }
    }
}
