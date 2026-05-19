package dev.dimster.availability;

import static org.junit.Assert.*;

import dev.dimster.MessageType;
import org.junit.Test;

public class AvailabilityPayloadTest {

    @Test
    public void encodeDecodeRoundtrip() {
        int partition = 5;
        long sequence = 42;
        long timestampMs = System.currentTimeMillis();

        byte[] encoded = AvailabilityPayload.encode(partition, sequence, timestampMs);
        assertEquals(AvailabilityPayload.PAYLOAD_SIZE, encoded.length);

        AvailabilityPayload.DecodedProbe decoded = AvailabilityPayload.decode(encoded);
        assertNotNull(decoded);
        assertEquals(partition, decoded.partition());
        assertEquals(sequence, decoded.sequence());
        assertEquals(timestampMs, decoded.produceTimestampMs());
    }

    @Test
    public void isAvailabilityMessageDetectsType() {
        byte[] avail = AvailabilityPayload.encode(0, 0, 0);
        assertTrue(AvailabilityPayload.isAvailabilityMessage(avail));

        byte[] normal = new byte[24];
        assertFalse(AvailabilityPayload.isAvailabilityMessage(normal));

        byte[] tooSmall = new byte[5];
        assertFalse(AvailabilityPayload.isAvailabilityMessage(tooSmall));
    }

    @Test
    public void decodeRejectsNonAvailabilityMessages() {
        byte[] normal = new byte[24]; // type = 0x00 (NORMAL)
        assertNull(AvailabilityPayload.decode(normal));
    }

    @Test
    public void decodeRejectsTooSmallPayload() {
        byte[] small = new byte[10];
        small[0] = MessageType.AVAILABILITY;
        assertNull(AvailabilityPayload.decode(small));
    }

    @Test
    public void firstByteIsAvailabilityType() {
        byte[] payload = AvailabilityPayload.encode(0, 0, 0);
        assertEquals(MessageType.AVAILABILITY, payload[0]);
    }
}
