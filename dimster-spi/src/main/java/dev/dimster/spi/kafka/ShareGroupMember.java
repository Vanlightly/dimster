package dev.dimster.spi.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * Per-member view of a share group, returned as part of {@link ShareGroupInfo}.
 *
 * <p>This is a deliberately narrow projection of Kafka's {@code ShareMemberDescription}
 * — only the fields we render in `dimster inspect` and any plausible future tooling
 * inside dimster-core. Wider Kafka types ({@code ShareMemberDescription},
 * {@code ShareMemberAssignment}) are 4.1+ only and cannot leak across the SPI
 * boundary because dimster-spi compiles against the 3.7.x baseline.
 *
 * @param consumerId       the share-group member id assigned by the broker
 * @param clientId         the client.id the member registered with
 * @param host             the member's host (typically "/<ip>")
 * @param assignment       partitions currently assigned to this member; empty
 *                         when the member is in an empty group or its assignment
 *                         has not yet been propagated.
 */
public record ShareGroupMember(
        String consumerId,
        String clientId,
        String host,
        Set<TopicPartition> assignment
) {
}
