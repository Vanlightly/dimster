package dev.dimster.spi.kafka;

import java.util.List;

/**
 * Snapshot of a share group's state and membership, returned by
 * {@link RawAdmin#describeShareGroup(String)}.
 *
 * <p>This is a deliberately narrow projection of Kafka's {@code ShareGroupDescription}
 * — only the fields we surface in `dimster inspect` (state, coordinator,
 * members, assignments). Avoiding 4.1+-only Kafka types in the SPI keeps
 * the dimster-spi module compilable against its pinned 3.7.x baseline so
 * the per-version impls remain free to evolve their Kafka surface without
 * shifting the SPI's compile classpath.
 *
 * @param groupId         the share-group id (matches the input to describeShareGroup)
 * @param state           textual state (e.g. "STABLE", "EMPTY", "DEAD") — pulled
 *                        from {@code groupState().toString()} so the SPI does not
 *                        need to mirror Kafka's GroupState enum, which itself
 *                        moved across versions.
 * @param coordinatorId   broker id hosting the group, or -1 when unknown.
 * @param members         per-member view; empty for an EMPTY share group.
 */
public record ShareGroupInfo(
        String groupId,
        String state,
        int coordinatorId,
        List<ShareGroupMember> members
) {
}
