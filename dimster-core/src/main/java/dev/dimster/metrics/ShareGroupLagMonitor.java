package dev.dimster.metrics;

import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.worker.commands.TopicConsumerGroup;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically polls the Kafka admin API for share group lag and exposes the
 * results as Prometheus gauge metrics. Kafka brokers don't expose share group
 * lag via JMX, so this component bridges that gap by calling the SPI admin's
 * {@link RawAdmin#shareGroupLag} on a background thread and making the latest
 * snapshot available to the PrometheusEndpoint.
 *
 * <p>Only constructed by {@code LocalWorker} when
 * {@code factory.capabilities().shareGroupsSupported()} is true. On 3.x builds
 * the monitor is never created, so the underlying SPI's
 * UnsupportedOperationException is never reached.
 */
public class ShareGroupLagMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ShareGroupLagMonitor.class);
    private static final long POLL_INTERVAL_SECONDS = 10;

    private final RawAdmin admin;
    private final ScheduledExecutorService scheduler;

    // The set of share groups to monitor, keyed by group name -> set of topics
    private final Map<String, Set<String>> groupTopics;

    // Latest lag snapshot: group -> (TopicPartition -> lag). Updated atomically via volatile.
    private volatile Map<String, Map<TopicPartition, Long>> lagSnapshot = Map.of();

    /**
     * @param admin              SPI admin handle - typically built by the caller via
     *                           {@code factory.newAdmin(config.commonProperties())} so
     *                           we own the lifetime of a separate admin client from
     *                           the one used by KafkaCluster (this one runs on a
     *                           dedicated 10-second polling thread).
     * @param topicConsumerGroups groups to monitor, with their topics.
     */
    public ShareGroupLagMonitor(RawAdmin admin, List<TopicConsumerGroup> topicConsumerGroups) {
        this.admin = admin;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "share-group-lag-monitor");
            t.setDaemon(true);
            return t;
        });

        // Deduplicate: build group -> topics mapping
        this.groupTopics = new LinkedHashMap<>();
        for (var tcg : topicConsumerGroups) {
            groupTopics.computeIfAbsent(tcg.consumerGroup(), k -> new LinkedHashSet<>())
                    .add(tcg.topic());
        }
    }

    public void start() {
        log.info("Starting share group lag monitor for groups: {}", groupTopics.keySet());
        scheduler.scheduleAtFixedRate(this::poll, 0, POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Writes per-partition and per-group total lag metrics in Prometheus text format.
     */
    public void writeLagMetrics(StringWriter writer, String hostname) {
        var snapshot = lagSnapshot;
        if (snapshot.isEmpty()) {
            return;
        }

        for (var groupEntry : snapshot.entrySet()) {
            String group = groupEntry.getKey();
            long totalLag = 0;

            for (var partitionEntry : groupEntry.getValue().entrySet()) {
                TopicPartition tp = partitionEntry.getKey();
                long lag = partitionEntry.getValue();
                totalLag += lag;

                writer.write("client_share_group_lag{host=\"" + hostname
                        + "\",group=\"" + group
                        + "\",topic=\"" + tp.topic()
                        + "\",partition=\"" + tp.partition()
                        + "\"} " + lag + "\n");
            }

            writer.write("client_share_group_lag_total{host=\"" + hostname
                    + "\",group=\"" + group
                    + "\"} " + totalLag + "\n");
        }
    }

    private void poll() {
        try {
            var newSnapshot = new LinkedHashMap<String, Map<TopicPartition, Long>>();

            for (var entry : groupTopics.entrySet()) {
                String group = entry.getKey();
                // RawAdmin computes lag server-side and returns the per-partition map.
                // One admin call per group, same as the original implementation.
                newSnapshot.put(group, admin.shareGroupLag(group));
            }

            lagSnapshot = newSnapshot;
        } catch (Exception e) {
            // Keep the previous snapshot on failure so metrics don't disappear
            log.warn("Failed to poll share group lag: {}", e);
        }
    }

    @Override
    public void close() {
        log.info("Stopping share group lag monitor");
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            admin.close();
        } catch (Throwable t) {
            log.warn("Failed closing share group lag monitor admin client", t);
        }
    }
}
