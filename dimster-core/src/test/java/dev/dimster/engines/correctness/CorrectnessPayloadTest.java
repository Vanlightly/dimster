package dev.dimster.engines.correctness;

import static org.junit.Assert.*;

import org.junit.Test;

public class CorrectnessPayloadTest {

    @Test
    public void encodeDecodeRoundtripWithoutCrc() {
        byte[] payload = new byte[64];
        CorrectnessPayload.encode(payload, 42, 12345L, 1000000L, false);

        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(42, decoded.producerId());
        assertEquals(12345L, decoded.sequence());
        assertEquals(1000000L, decoded.timestampMs());
        assertTrue(decoded.crcValid());
    }

    @Test
    public void encodeDecodeRoundtripWithCrc() {
        byte[] payload = new byte[128];
        CorrectnessPayload.encode(payload, 7, 99999L, 2000000L, true);

        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(7, decoded.producerId());
        assertEquals(99999L, decoded.sequence());
        assertEquals(2000000L, decoded.timestampMs());
        assertTrue(decoded.crcValid());
    }

    @Test
    public void crcDetectsCorruption() {
        byte[] payload = new byte[32];
        CorrectnessPayload.encode(payload, 1, 100L, 3000000L, true);

        // Corrupt a byte in the header (sequence field, after type byte)
        payload[6] ^= 0xFF;

        var decoded = CorrectnessPayload.decode(payload);
        assertFalse(decoded.crcValid());
    }

    @Test
    public void minimumSizeWithoutCrc() {
        byte[] payload = new byte[CorrectnessPayload.HEADER_SIZE];
        CorrectnessPayload.encode(payload, 0, 0L, 0L, false);

        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(0, decoded.producerId());
        assertEquals(0L, decoded.sequence());
        assertTrue(decoded.crcValid());
    }

    @Test
    public void minimumSizeWithCrc() {
        byte[] payload = new byte[CorrectnessPayload.HEADER_WITH_CRC_SIZE];
        CorrectnessPayload.encode(payload, 0, 0L, 0L, true);

        var decoded = CorrectnessPayload.decode(payload);
        assertTrue(decoded.crcValid());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallWithoutCrc() {
        byte[] payload = new byte[CorrectnessPayload.HEADER_SIZE - 1];
        CorrectnessPayload.encode(payload, 0, 0L, 0L, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallWithCrc() {
        byte[] payload = new byte[CorrectnessPayload.HEADER_WITH_CRC_SIZE - 1];
        CorrectnessPayload.encode(payload, 0, 0L, 0L, true);
    }

    @Test
    public void tooSmallPayloadDecodeReturnsInvalid() {
        var decoded = CorrectnessPayload.decode(new byte[10]);
        assertEquals(-1, decoded.producerId());
        assertFalse(decoded.crcValid());
    }

    @Test
    public void noCrcPresentWhenNoCrcFlag() {
        byte[] payload = new byte[64];
        CorrectnessPayload.encode(payload, 5, 50L, 500L, false);
        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(5, decoded.producerId());
        assertEquals(50L, decoded.sequence());
        assertTrue(decoded.crcValid());
    }

    @Test
    public void nonCorrectnessMessageReturnsInvalid() {
        byte[] payload = new byte[64];
        payload[0] = 0x00; // MessageType.NORMAL
        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(-1, decoded.producerId());
        assertFalse(CorrectnessPayload.isCorrectnessMessage(payload));
    }

    @Test
    public void isCorrectnessMessageDetectsType() {
        byte[] correctness = new byte[32];
        CorrectnessPayload.encode(correctness, 1, 0L, 0L, false);
        assertTrue(CorrectnessPayload.isCorrectnessMessage(correctness));

        byte[] normal = new byte[32];
        assertFalse(CorrectnessPayload.isCorrectnessMessage(normal));
    }

    @Test
    public void largeSequenceNumbers() {
        long bigSeq = Long.MAX_VALUE - 1;
        byte[] payload = new byte[32];
        CorrectnessPayload.encode(payload, Integer.MAX_VALUE, bigSeq, Long.MAX_VALUE, true);

        var decoded = CorrectnessPayload.decode(payload);
        assertEquals(Integer.MAX_VALUE, decoded.producerId());
        assertEquals(bigSeq, decoded.sequence());
        assertEquals(Long.MAX_VALUE, decoded.timestampMs());
        assertTrue(decoded.crcValid());
    }

    @Test
    public void encodePreservesTrailingBytes() {
        // Verify that encoding only writes the header, leaving trailing bytes unchanged
        byte[] payload = new byte[64];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) 0xAB;
        }
        CorrectnessPayload.encode(payload, 1, 1L, 1L, false);

        // Bytes beyond the header should still be 0xAB
        for (int i = CorrectnessPayload.HEADER_SIZE; i < payload.length; i++) {
            assertEquals("Byte " + i + " should be preserved", (byte) 0xAB, payload[i]);
        }
    }
}
