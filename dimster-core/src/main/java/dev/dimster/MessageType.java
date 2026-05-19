package dev.dimster;

/**
 * First-byte marker for all message payloads. Allows consumers to identify
 * the payload format without out-of-band signalling.
 */
public final class MessageType {
    public static final byte NORMAL      = 0x00;
    public static final byte CORRECTNESS = 0x01;
    public static final byte AVAILABILITY = 0x02;
    public static final byte PROBE       = 0x03;

    /** Human-readable name for the given first-byte marker.
     *  Accepts int so callers can pass -1 for missing/empty payloads. */
    public static String nameOf(int marker) {
        return switch (marker) {
            case NORMAL       -> "normal";
            case CORRECTNESS  -> "correctness";
            case AVAILABILITY -> "availability";
            case PROBE        -> "probe";
            default           -> "unknown(0x" + String.format("%02x", marker) + ")";
        };
    }

    private MessageType() {}
}
