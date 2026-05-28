package dev.dimster.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.dimster.MessageType;
import dev.dimster.spi.kafka.AckType;
import dev.dimster.spi.kafka.RawShareConsumer;
import dev.dimster.spi.kafka.ShareAckListener;
import dev.dimster.utils.Sleeper;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaShareConsumerWrapper implements AutoCloseable, ShareAckListener, KafkaMetricsSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaShareConsumerWrapper.class);
    private static final String MANUAL_COMMIT_LINGER_CONFIG = "dimster.commit.linger.ms";
    private static final String MANUAL_COMMIT_MODE = "dimster.commit.mode";
    private static final String POLL_TIMEOUT_CONFIG = "dimster.poll.timeout.ms";
    private static final String POLL_TIMEOUT_DEFAULT = "100";

    // SPI handle to the underlying kafka-clients share consumer. The concrete
    // impl is chosen at startup via ServiceLoader (KafkaClientFactory) and is
    // only available on dimster builds against Kafka 4.2+; pre-4.2 builds fail
    // fast at workload-build time via the factory's capabilities() check.
    private final RawShareConsumer<String, byte[]> consumer;
    private final ConsumerCallback callback;
    private final String name;
    private final boolean explicitAck;
    private final ExecutorService executor;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private volatile boolean running = false;
    private volatile String failureReason = null;

    private enum ManualCommitMode {
        CommitSync,
        CommitAsync,
        Poll;
    }

    private final long manualCommitLingerMs;
    private final ManualCommitMode manualCommitMode;
    private final Duration pollTimeout;
    private long lastCommitTs = 0;

    private volatile int procMsMin;
    private volatile int procMsMax;
    private volatile int procConcurrency;
    private volatile ProcessingTimeSimulator processingTimeSimulator;

    /** When true, every consumed record is logged with its partition + timestamp.
     *  Wired in from {@code engineConfig.printMessages}; the upstream
     *  coordinator already enforces the rate guard, so this wrapper just obeys. */
    private final boolean printMessages;

    /**
     * @param name canonical client name from {@code VirtualConsumer.name()}
     *     (e.g. {@code "consumer-{topic}-{group}-i{idx}"}). Same identity
     *     the planner uses, so {@code LocalWorker.removeConsumers} can
     *     look up wrappers by name.
     */
    public KafkaShareConsumerWrapper(RawShareConsumer<String, byte[]> consumer,
                                     Properties consumerConfig,
                                     ConsumerCallback callback,
                                     String topic,
                                     String name,
                                     int procMsMin,
                                     int procMsMax,
                                     int procConcurrency,
                                     boolean printMessages) {
        this.consumer = consumer;
        this.callback = callback;
        this.name = name;
        this.printMessages = printMessages;

        this.explicitAck = "explicit".equalsIgnoreCase(
                (String) consumerConfig.getOrDefault("share.acknowledgement.mode", "implicit"));
        this.manualCommitLingerMs = Long.parseLong(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_LINGER_CONFIG, "0"));
        this.manualCommitMode = ManualCommitMode.valueOf(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_MODE, "Poll"));
        this.pollTimeout = Duration.ofMillis(Long.parseLong(
                (String) consumerConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)));
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procConcurrency = procConcurrency;
        this.processingTimeSimulator = new ProcessingTimeSimulator(procMsMin, procMsMax, procConcurrency);

        if (explicitAck) {
            consumer.setAckCallback(this);
        }
        consumer.subscribe(Set.of(topic));

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
        this.processingTimeSimulator = new ProcessingTimeSimulator(minMs, maxMs, procConcurrency);
    }

    private void pollLoop() {
        log.info("Share consumer poll loop started: {}", name);
        try {
            while (!closing.get()) {
                try {
                    var records = consumer.poll(pollTimeout);
                    if (closing.get()) return;
                    running = true;
                    Instant deliveryTime = Instant.now();
                    callback.pollCompleted(records.count());
                    callback.batchReceived(name, records.count());

                    // Convert records to list for index-based access
                    var recordList = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>();
                    for (var record : records) {
                        recordList.add(record);
                    }

                    // Process the records with simulated processing time --------------
                    processingTimeSimulator.processBatch(
                            recordList.size(),
                            closing,
                            callback,
                            (startIdx, endIdx) -> {
                                // Process messages in range [startIdx, endIdx)
                                for (int i = startIdx; i < endIdx; i++) {
                                    var record = recordList.get(i);

                                    // Print the messages if configured
                                    if (printMessages) {
                                        long e2eLatencyMs = System.currentTimeMillis() - record.timestamp();
                                        byte[] value = record.value();
                                        String msgType = MessageType.nameOf(value != null && value.length > 0 ? value[0] : -1);
                                        log.info("CONSUMED topic={} partition={} offset={} timestamp={} e2eLatencyMs={} type={}",
                                                record.topic(), record.partition(),
                                                record.offset(), record.timestamp(), e2eLatencyMs, msgType);
                                    }

                                    // Records stats
                                    callback.messageReceived(record.value(), record.topic(), record.partition(),
                                            record.timestamp(), deliveryTime);

                                    // Maybe acknowledge
                                    // Note: acknowledge(ACCEPT) is local-only (no network I/O),
                                    // so its position relative to the aggregate sleep is immaterial.
                                    if (explicitAck) {
                                        consumer.acknowledge(record, AckType.ACCEPT);
                                    }
                                }
                            }
                    );

                    // Maybe do manual commit --------------
                    if (manualCommitMode != ManualCommitMode.Poll) {
                        long now = System.currentTimeMillis();
                        if (now - lastCommitTs >= manualCommitLingerMs) {
                            if (manualCommitMode == ManualCommitMode.CommitAsync) {
                                consumer.commitAsync();
                                callback.commitAsyncCompleted();
                            } else if (manualCommitMode == ManualCommitMode.CommitSync) {
                                long startNanos = System.nanoTime();
                                consumer.commitSync();
                                long latencyMicros = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                                        System.nanoTime() - startNanos);
                                callback.commitSyncCompleted(latencyMicros);
                            }
                            lastCommitTs = now;
                        }
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!closing.get()) throw e;
                }
            }
        } catch (Throwable t) {
            failureReason = t.getClass().getName() + ": " + t.getMessage();
            log.error("Fatal error in share consumer poll loop", t);
        } finally {
            // Perform one last commit before close -------------
            try {
                consumer.commitSync();
            } catch (Throwable t) {
                log.error("Failed while performing final commitSync", t);
            }
            running = false;
            log.info("Share consumer poll loop ended: {}", name);
        }
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

    /**
     * SPI ack-commit callback. The 4.2 impl flattens Kafka's
     * Map&lt;TopicIdPartition, Set&lt;Long&gt;&gt; to Map&lt;TopicPartition, Set&lt;Long&gt;&gt;
     * before invoking us; the topic id was never read here anyway.
     */
    @Override
    public void onComplete(Map<TopicPartition, Set<Long>> committedOffsets, Throwable e) {
        if (e != null) {
            log.warn("Error in share group acknowledgement commit", e);
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
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Consumer {} executor did not exit it's poll loop, forcing shutdown", name);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Consumer {} interrupted while awaiting executor shutdown", name);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            // The 60-second close timeout lives inside RawShareConsumer's
            // per-version impls now; close() on the SPI is parameterless.
            consumer.close();
        } finally {
            closeFuture.complete(null);
        }
    }
}
