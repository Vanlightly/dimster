package dev.dimster;

/**
 * Consumer processing mode: BLOCKING vs CONTINUOUS.
 *
 * <p><b>BLOCKING</b> mode (default): {@code processBatch()} blocks the poll loop
 * until all records in the batch complete processing. Records process in receive order.
 * Simpler and safer, but limits throughput when simulating high parallelism with long
 * processing times.
 *
 * <p><b>CONTINUOUS</b> mode: Records process asynchronously via {@code DelayQueue},
 * allowing the poll loop to continue fetching without blocking. Enables sustained
 * high throughput (e.g., 60K msg/s with 1000 consumers, 20s processing time).
 *
 * <p><b>IMPORTANT</b>: CONTINUOUS mode is ONLY compatible with {@code CONSUMER_GROUP}.
 * It cannot be used with {@code SHARE_GROUP} consumers due to the {@code currentBatch}
 * limitation — share groups can only acknowledge records from the most recent poll batch,
 * which combined with out-of-order completion creates redelivery starvation.
 *
 * @see dev.dimster.kafka.ProcessingTimeSimulator
 * @see dev.dimster.kafka.KafkaConsumerWrapper
 */
public enum ConsumerProcMode {
    /** Synchronous processing that blocks the poll loop until batch completes. */
    BLOCKING,

    /** Asynchronous processing via DelayQueue, enabling continuous polling. */
    CONTINUOUS
}
