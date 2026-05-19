package dev.dimster.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.dimster.MessageType;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.utils.Sleeper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWrapper implements AutoCloseable, OffsetCommitCallback, KafkaMetricsSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWrapper.class);
    private static final String MANUAL_COMMIT_LINGER_CONFIG = "dimster.commit.linger.ms";
    private static final String MANUAL_COMMIT_MODE = "dimster.commit.mode";
    private static final String POLL_TIMEOUT_CONFIG = "dimster.poll.timeout.ms";
    private static final String POLL_TIMEOUT_DEFAULT = "100";

    /**
     * Threshold (in milliseconds) below which per-record processing-time sleeps
     * are aggregated into a single sleep at the end of each batch.
     *
     * Why: Thread.sleep(1) on most platforms has ~1-3ms variance due to OS
     * scheduling granularity. With many records per batch, that variance
     * compounds and dominates the simulated processing time. Aggregating into
     * one sleep per batch amortizes the OS slop across all records, giving a
     * far more stable effective per-record proc time. The decision uses the
     * midpoint of [procMsMin, procMsMax] so that random ranges are evaluated
     * by their average.
     *
     * Used by both KafkaConsumerWrapper and KafkaShareConsumerWrapper, and
     * referenced by engines so they can warn the user when aggregate mode
     * activates.
     */
    public static final int AGGREGATE_PROC_TIME_THRESHOLD_MS = 10;

    /**
     * Engines call this right after logging a test point's config so that
     * users see clearly when the consumer wrappers will switch to aggregate
     * sleep mode for this test. No-op when proc time is zero or above the
     * threshold.
     */
    public static void warnIfAggregateProcTime(Logger logger, int procMsMin, int procMsMax) {
        if (procMsMax > 0
                && ((procMsMin + procMsMax) / 2) < AGGREGATE_PROC_TIME_THRESHOLD_MS) {
            logger.warn("Aggregate processing-time mode active: per-record proc time midpoint < {}ms"
                    + " (min={}ms, max={}ms). Per-record Thread.sleep is replaced with one"
                    + " aggregate sleep per batch to reduce variance. Per-record processing-time"
                    + " metrics are not recorded in this mode.",
                    AGGREGATE_PROC_TIME_THRESHOLD_MS, procMsMin, procMsMax);
        }
    }

    // SPI handle to the underlying kafka-clients consumer. The concrete impl is
    // chosen at startup via ServiceLoader (see KafkaClientFactory) and bound to
    // whichever Kafka version this dimster build was compiled against.
    private final RawConsumer<String, byte[]> consumer;
    private final ConsumerCallback callback;
    private final String name;
    private final ExecutorService executor;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private volatile boolean running = false;
    private volatile String failureReason = null;

    private enum ManualCommitMode {
        CommitSync,
        CommitAsync
    }

    private final long manualCommitLingerMs;
    private final ManualCommitMode manualCommitMode;
    private final boolean autoCommit;
    private final Duration pollTimeout;
    private long lastCommitTs = 0;

    private volatile int procMsMin;
    private volatile int procMsMax;

    /** When true, every consumed record is logged with its partition + timestamp.
     *  Wired in from {@code engineConfig.printMessages}; the upstream
     *  coordinator already enforces the rate guard, so this wrapper just obeys. */
    private final boolean printMessages;

    public KafkaConsumerWrapper(RawConsumer<String, byte[]> consumer,
                                Properties consumerConfig,
                                ConsumerCallback callback,
                                String topic,
                                String name,
                                int procMsMin,
                                int procMsMax) {
        this(consumer, consumerConfig, callback, topic, name, procMsMin, procMsMax, null, false);
    }

    /**
     * @param name canonical client name from {@code VirtualConsumer.name()}
     *     (e.g. {@code "consumer-{topic}-{group}-i{idx}"}). Used by
     *     {@link #getName()} for log lines, metric labels, and — critically
     *     — by {@code LocalWorker.removeConsumers} to look up the wrapper
     *     by the same identity the planner uses.
     */
    public KafkaConsumerWrapper(RawConsumer<String, byte[]> consumer,
                                Properties consumerConfig,
                                ConsumerCallback callback,
                                String topic,
                                String name,
                                int procMsMin,
                                int procMsMax,
                                List<TopicPartition> staticPartitions,
                                boolean printMessages) {
        this.consumer = consumer;
        this.callback = callback;
        this.name = name;
        this.printMessages = printMessages;

        this.manualCommitLingerMs = Long.parseLong(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_LINGER_CONFIG, "0"));
        this.manualCommitMode = ManualCommitMode.valueOf(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_MODE, "CommitAsync"));
        this.autoCommit = Boolean.parseBoolean(
                (String) consumerConfig.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));
        this.pollTimeout = Duration.ofMillis(Long.parseLong(
                (String) consumerConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)));
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;

        if (staticPartitions != null) {
            consumer.assign(staticPartitions);
            log.info("Consumer {} using static partition assignment: {}", name, staticPartitions);
        } else {
            consumer.subscribe(List.of(topic));
        }

        // this.pollThread = Thread.ofVirtual()
        //         .name(name)
        //         .start(this::pollLoop);
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
//            t.setDaemon(true);
            return t;
        });
        this.executor.submit(this::pollLoop);
    }

    /** Adjust the simulated per-record processing time at runtime. Thread-safe. */
    public void setProcessingTime(int minMs, int maxMs) {
        this.procMsMin = minMs;
        this.procMsMax = maxMs;
    }

    private void pollLoop() {
        log.info("Classic consumer poll loop started: {}", name);
        try {
            while (!closing.get()) {
                try {
                    var records = consumer.poll(pollTimeout);
                    if (closing.get()) return;

                    running = true;
                    Instant deliveryTime = Instant.now();
                    callback.pollCompleted(records.count());
                    callback.batchReceived(name, records.count());

                    // Decide once per batch whether to aggregate per-record sleeps.
                    // Aggregate mode trades per-record proc-time-metric granularity
                    // for a much more stable effective per-record proc time when the
                    // target is small enough that Thread.sleep variance dominates.
                    // See AGGREGATE_PROC_TIME_THRESHOLD_MS for the rationale.
                    boolean useAggregate = procMsMax > 0
                            && ((procMsMin + procMsMax) / 2) < AGGREGATE_PROC_TIME_THRESHOLD_MS;
                    long aggregateSleepMs = 0;

                    // Process the records --------------
                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    for (var record : records) {
                        if (printMessages) {
                            long e2eLatencyMs = System.currentTimeMillis() - record.timestamp();
                            byte[] value = record.value();
                            String msgType = MessageType.nameOf(value != null && value.length > 0 ? value[0] : -1);
                            log.info("CONSUMED topic={} partition={} offset={} timestamp={} e2eLatencyMs={} type={}",
                                    record.topic(), record.partition(),
                                    record.offset(), record.timestamp(), e2eLatencyMs, msgType);
                        }

                        callback.messageReceived(record.value(), record.topic(),
                                record.partition(), record.timestamp(),
                                deliveryTime);

                        // Add processing time --------------
                        if (procMsMax > 0) {
                            long sleepMs = (procMsMin == procMsMax)
                                    ? procMsMin
                                    : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);

                            if (useAggregate) {
                                // Defer sleep to a single aggregate Thread.sleep
                                // after the per-record loop. Per-record metrics
                                // are intentionally not recorded in this mode -
                                // the coordinator emits a WARN at test start.
                                aggregateSleepMs += sleepMs;
                            } else {
                                long startNanos = System.nanoTime();
                                long remaining = sleepMs;

                                // Periodically check for close request --------------
                                while (remaining > 0 && !closing.get()) {
                                    Thread.sleep(Math.min(remaining, 1000));
                                    remaining = sleepMs - java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                            System.nanoTime() - startNanos);
                                }

                                long latencyMicros = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                                        System.nanoTime() - startNanos);
                                callback.processingTimeCompleted(latencyMicros);
                            }
                        }

                        // Maybe record offsets for manual commit --------------
                        if (!autoCommit) {
                            offsetMap.put(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset()));
                        }
                    }

                    // Aggregate per-batch sleep, performed before any commit so
                    // commits reflect work that has "completed".
                    if (useAggregate && aggregateSleepMs > 0) {
                        if (Sleeper.sleepOrStop(closing, aggregateSleepMs, false)) return;
                    }

                    // Maybe do manual commit --------------
                    long now = System.currentTimeMillis();
                    if (!autoCommit && !offsetMap.isEmpty()
                            && now - lastCommitTs >= manualCommitLingerMs) {
                        if (manualCommitMode == ManualCommitMode.CommitAsync) {
                            consumer.commitAsync(offsetMap, this);
                            callback.commitAsyncCompleted();
                        } else {
                            long startNanos = System.nanoTime();
                            consumer.commitSync(offsetMap, Duration.ofSeconds(30));
                            long latencyMicros = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                                    System.nanoTime() - startNanos);
                            callback.commitSyncCompleted(latencyMicros);
                        }
                        lastCommitTs = now;
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!closing.get()) throw e;
                }
            }
        } catch (Throwable t) {
            failureReason = t.getClass().getName() + ": " + t.getMessage();
            log.error("Fatal error in classic consumer poll loop", t);
        } finally {
            running = false;
        }
        log.info("Classic consumer poll loop ended: {}", name);
    }

    public String getName() {
        return name;
    }

    public boolean isRunning() {
        return running;
    }

    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public Map<MetricName, ? extends Metric> kafkaMetrics() {
        return consumer.metrics();
    }

    @Override
    public String clientName() {
        return name;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            log.warn("Error committing offsets", e);
            callback.error();
        }
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            log.info("Consumer close of {} already in progress, waiting", name);
            closeFuture.join();
            return;
        }
        try {
            consumer.wakeup();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Consumer {} executor did not terminate in time, forcing shutdown", name);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Consumer {} interrupted while awaiting executor shutdown", name);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            // The 60-second close timeout lives inside RawConsumer's per-version
            // impls now; close() on the SPI is parameterless.
            consumer.close();
        } finally {
            closeFuture.complete(null);
        }
    }
}
