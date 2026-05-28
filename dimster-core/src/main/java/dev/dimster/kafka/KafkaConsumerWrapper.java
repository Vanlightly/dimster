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
    private volatile int procConcurrency;
    private volatile ProcessingTimeSimulator processingTimeSimulator;

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
                                int procMsMax,
                                int procConcurrency) {
        this(consumer, consumerConfig, callback, topic, name, procMsMin, procMsMax, procConcurrency, null, false);
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
                                int procConcurrency,
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
        this.procConcurrency = procConcurrency;
        this.processingTimeSimulator = new ProcessingTimeSimulator(procMsMin, procMsMax, procConcurrency);

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
        this.processingTimeSimulator = new ProcessingTimeSimulator(minMs, maxMs, procConcurrency);
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

                    // Convert records to list for index-based access
                    var recordList = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>();
                    for (var record : records) {
                        recordList.add(record);
                    }

                    // Process the records with simulated processing time --------------
                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    processingTimeSimulator.processBatch(
                            recordList.size(),
                            closing,
                            callback,
                            deliveryTime,
                            (messageIdx, simulatedProcessingStartTime) -> {
                                // Process single message with its simulated processing start time
                                var record = recordList.get(messageIdx);

                                if (printMessages) {
                                    long e2eLatencyMs = System.currentTimeMillis() - record.timestamp();
                                    byte[] value = record.value();
                                    String msgType = MessageType.nameOf(value != null && value.length > 0 ? value[0] : -1);
                                    log.info("CONSUMED topic={} partition={} offset={} timestamp={} e2eDeliveryLatencyMs={} type={}",
                                            record.topic(), record.partition(),
                                            record.offset(), record.timestamp(), e2eLatencyMs, msgType);
                                }

                                callback.messageReceived(record.value(), record.topic(),
                                        record.partition(), record.timestamp(),
                                        deliveryTime, simulatedProcessingStartTime);

                                // Maybe record offsets for manual commit
                                if (!autoCommit) {
                                    offsetMap.put(
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset()));
                                }
                            }
                    );

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
