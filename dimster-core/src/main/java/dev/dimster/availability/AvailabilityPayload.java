package dev.dimster.availability;

import java.nio.ByteBuffer;

import dev.dimster.MessageType;

/**
 * Codec for availability probe message payloads.
 *
 * Format:
 *   [1 byte: type (AVAILABILITY)][4 bytes: partition][8 bytes: sequence][8 bytes: produceTimestampMs]
 *
 * Total: 21 bytes.
 */
public final class AvailabilityPayload {

    public static final int PAYLOAD_SIZE = 21;

    private AvailabilityPayload() {}

    public record DecodedProbe(int partition, long sequence, long produceTimestampMs) {}

    public static byte[] encode(int partition, long sequence, long produceTimestampMs) {
        byte[] payload = new byte[PAYLOAD_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.put(MessageType.AVAILABILITY);
        buf.putInt(partition);
        buf.putLong(sequence);
        buf.putLong(produceTimestampMs);
        return payload;
    }

    public static DecodedProbe decode(byte[] payload) {
        if (payload.length < PAYLOAD_SIZE) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte type = buf.get();
        if (type != MessageType.AVAILABILITY) {
            return null;
        }

        int partition = buf.getInt();
        long sequence = buf.getLong();
        long produceTimestampMs = buf.getLong();
        return new DecodedProbe(partition, sequence, produceTimestampMs);
    }

    public static boolean isAvailabilityMessage(byte[] payload) {
        return payload.length >= PAYLOAD_SIZE && payload[0] == MessageType.AVAILABILITY;
    }
}
