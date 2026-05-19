package dev.dimster.spi.kafka;

/**
 * Per-partition share-group offset state, returned by
 * {@link RawAdmin#shareGroupOffsets(String)}.
 *
 * <p>Two fields:
 * <ul>
 *   <li>{@code startOffset} — the share-partition start offset (SPSO),
 *       i.e. the offset of the next record waiting to be processed by the
 *       share group on this partition. Always populated.</li>
 *   <li>{@code lag} — records behind the latest produced offset on the
 *       partition. Populated by the broker on 4.2+ and computed
 *       client-side on 4.1; defaults to 0 when the broker has no lag
 *       value for the partition (e.g. a brand-new share group with no
 *       activity yet).</li>
 * </ul>
 *
 * <p>Why this exists alongside {@link RawAdmin#shareGroupLag} rather than
 * replacing it: the metric polling and engine call sites only ever need
 * lag, and the existing {@code shareGroupLag(String) -> Map<TopicPartition, Long>}
 * shape is a simpler return for that hot path. {@code shareGroupOffsets}
 * is the diagnostic/inspect path that wants both numbers in one RPC.
 */
public record ShareGroupOffsetInfo(long startOffset, long lag) {
}
