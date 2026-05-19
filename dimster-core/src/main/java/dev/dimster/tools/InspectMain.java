package dev.dimster.tools;

import dev.dimster.kafka.KafkaCluster;
import dev.dimster.kafka.KafkaSpi;
import dev.dimster.spi.kafka.Capabilities;
import dev.dimster.spi.kafka.GroupKind;
import dev.dimster.spi.kafka.KafkaClientFactory;
import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.spi.kafka.ShareGroupInfo;
import dev.dimster.spi.kafka.ShareGroupMember;
import dev.dimster.spi.kafka.ShareGroupOffsetInfo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Standalone CLI tool that prints a read-only snapshot of a Kafka cluster's
 * topics, consumer groups, and (when supported) share groups using Kafka's
 * AdminClient. Designed to run inside the dimster coordinator pod so the
 * Rust CLI can offer `dimster inspect` against external Kafka clusters
 * (where there is no `kafka-0` pod to kubectl-exec into).
 *
 * <h2>Why this class lives in dimster-core</h2>
 *
 * It needs the SPI's {@code RawAdmin} for share-group operations (whose
 * surface differs across Kafka versions) and the {@code KafkaSpi} ServiceLoader
 * binding to pick up the per-version impl bundled into the running tarball.
 * Both are dimster-core concerns. dimster-core itself is already on the
 * coordinator's runtime classpath via {@code lib/*}, so no packaging changes
 * were needed beyond the new {@code bin/benchmark-inspect} launcher.
 *
 * <h2>Architecture: hybrid AdminClient + RawAdmin</h2>
 *
 * Stable describe APIs ({@code listTopics}, {@code describeTopics},
 * {@code describeConfigs}, {@code describeConsumerGroups},
 * {@code listConsumerGroupOffsets}, {@code listOffsets}) go directly through
 * {@link AdminClient}. These have been API-stable since well before Kafka 3.x
 * and are present in dimster-spi's pinned 3.7.x baseline that flows through
 * to dimster-core's compile classpath.
 *
 * The version-sensitive share-group operations go through the existing
 * {@link RawAdmin} SPI, which gates per-version availability via
 * {@link Capabilities#shareGroupsSupported()} and throws
 * {@link UnsupportedOperationException} on pre-4.2 impls. We intentionally
 * do NOT use {@code AdminClient.describeShareGroups} or
 * {@code AdminClient.listShareGroupOffsets} directly because those are 4.2+
 * surface and would not compile against the SPI's 3.7.x baseline.
 *
 * <h2>Output format</h2>
 *
 * Best-effort reproduction of the original `dimster inspect` shell-script
 * sections — the same TOPICS / CONSUMER GROUPS / SHARE GROUPS headers and
 * per-group blocks — but section bodies are formatted from AdminClient
 * results rather than mimicking `kafka-*.sh` output byte-for-byte.
 *
 * <h2>Filter flags</h2>
 *
 * Output is the full snapshot by default. Five flags narrow it down and may
 * be combined freely:
 * <ul>
 *   <li>{@code --dimster-only} — restrict to dimster-created resources only:
 *       topics whose name starts with {@link KafkaCluster#TOPIC_PREFIX} and
 *       groups whose id starts with {@link KafkaCluster#TEST_GROUP_PREFIX}.
 *       No effect on the METADATA QUORUM section (the quorum is what it is).</li>
 *   <li>{@code --topics} — emit only the TOPICS section.</li>
 *   <li>{@code --group-offsets} — within group sections, emit only the
 *       {@code OFFSETS} subsection of each group block (the per-group header
 *       is still shown).</li>
 *   <li>{@code --group-members} — within group sections, emit only the
 *       {@code MEMBERS} subsection of each group block.</li>
 *   <li>{@code --metadata-quorum} — emit only the METADATA QUORUM section
 *       (KRaft quorum leader/voters/observers from
 *       {@link AdminClient#describeMetadataQuorum()}).</li>
 * </ul>
 * If any of the section-selector flags
 * ({@code --topics} / {@code --group-offsets} / {@code --group-members} /
 * {@code --metadata-quorum}) is set, only the named pieces render. If none
 * are set, all sections render in full. {@code --dimster-only} is orthogonal
 * — it filters within whatever sections are shown.
 *
 * <h2>Failure handling</h2>
 *
 * Each per-group describe call is wrapped in try/catch so one transient
 * RPC failure does not abort the whole inspect. Per-section failures print
 * a single `(error: ...)` line and continue.
 */
public final class InspectMain {

    /** Timeout on every blocking AdminClient call. Matches what V4_2_0Admin uses. */
    private static final long ADMIN_TIMEOUT_SECONDS = 30;

    /** Maximum topics per describeTopics batch — keeps individual RPCs reasonable on huge clusters. */
    private static final int DESCRIBE_BATCH_SIZE = 200;

    /** Above this topic count we print a one-line "may take a moment" hint before describing. */
    private static final int LARGE_TOPIC_THRESHOLD = 500;

    private static final String SECTION_RULE =
            "================================================================================";

    private static final String USAGE =
            "Usage: InspectMain --props <file>|- "
            + "[--dimster-only] [--topics] [--group-offsets] [--group-members] "
            + "[--metadata-quorum]";

    /**
     * Combination of filter flags resolved from the command line.
     *
     * <p>Two orthogonal axes:
     * <ul>
     *   <li><b>Section selectors</b> ({@code showTopics}, {@code showGroupOffsets},
     *       {@code showGroupMembers}): if any are true, only the named pieces
     *       render. If all three are false, all three sections render in full
     *       (the no-flag default). The {@code *Requested()} accessors below
     *       encode this rule so callers don't have to recompute it.</li>
     *   <li><b>Resource filter</b> ({@code dimsterOnly}): independently restricts
     *       which topics and groups appear within whatever sections render.</li>
     * </ul>
     */
    record Filters(boolean dimsterOnly,
                   boolean showTopics,
                   boolean showGroupOffsets,
                   boolean showGroupMembers,
                   boolean showMetadataQuorum) {
        /** True when the user passed none of the section-selector flags — render everything. */
        boolean noSectionFlagsSet() {
            return !showTopics && !showGroupOffsets && !showGroupMembers && !showMetadataQuorum;
        }
        boolean topicsSectionRequested() {
            return showTopics || noSectionFlagsSet();
        }
        /** True if either the OFFSETS or MEMBERS subsection is requested for a group. */
        boolean groupSectionsRequested() {
            return showGroupOffsets || showGroupMembers || noSectionFlagsSet();
        }
        boolean offsetsSubsectionRequested() {
            return showGroupOffsets || noSectionFlagsSet();
        }
        boolean membersSubsectionRequested() {
            return showGroupMembers || noSectionFlagsSet();
        }
        boolean metadataQuorumSectionRequested() {
            return showMetadataQuorum || noSectionFlagsSet();
        }
    }

    /** Result of {@link #parseArgs}: kafka client props + resolved filter flags. */
    record ParsedArgs(Properties props, Filters filters) {}

    private InspectMain() {
        // tool entrypoint only
    }

    public static void main(String[] args) throws Exception {
        ParsedArgs parsed;
        try {
            parsed = parseArgs(args);
        } catch (IllegalArgumentException ex) {
            // Single exit point for arg-validation errors so parseArgs stays
            // pure (and unit-testable) — it throws, main() turns that into
            // the user-facing usage message + non-zero exit code.
            System.err.println(ex.getMessage());
            System.err.println(USAGE);
            System.exit(2);
            return;
        }
        Properties props = parsed.props();
        Filters filters = parsed.filters();

        // Resolves the per-version SPI factory and logs the bound kafka-clients
        // version. Same call site as Dimster.main so both tools see the same
        // version-binding behavior.
        KafkaClientFactory factory = KafkaSpi.factory();
        Capabilities caps = factory.capabilities();

        // Two clients on purpose:
        //  - RawAdmin gives version-correct share-group operations.
        //  - AdminClient gives the broader describe surface we need (topic
        //    configs, consumer-group state/members) without forcing those
        //    methods through the SPI just for an inspect tool.
        try (RawAdmin raw = factory.newAdmin(props);
             AdminClient admin = AdminClient.create(props)) {

            // Conditionally render each section. Blank-line separators are only
            // printed between sections that actually rendered, so e.g.
            // `--topics` doesn't leave two trailing blank lines.
            boolean printedAnything = false;
            if (filters.topicsSectionRequested()) {
                printTopics(admin, filters);
                printedAnything = true;
            }
            if (filters.groupSectionsRequested()) {
                if (printedAnything) System.out.println();
                printConsumerGroups(admin, raw, filters);
                System.out.println();
                printShareGroups(raw, caps, filters);
                printedAnything = true;
            }
            if (filters.metadataQuorumSectionRequested()) {
                if (printedAnything) System.out.println();
                printMetadataQuorum(admin);
            }
        }
    }

    /**
     * Loads Kafka client properties from a file or stdin and resolves filter
     * flags. Throws {@link IllegalArgumentException} on missing {@code --props}
     * or unknown flags so callers (main + tests) can decide how to surface the
     * failure. Usage: {@code --props <path>|-} plus any combination of
     * {@code --dimster-only}, {@code --topics}, {@code --group-offsets},
     * {@code --group-members}.
     */
    static ParsedArgs parseArgs(String[] args) throws IOException {
        String source = null;
        boolean dimsterOnly = false;
        boolean showTopics = false;
        boolean showGroupOffsets = false;
        boolean showGroupMembers = false;
        boolean showMetadataQuorum = false;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--props":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--props requires a value");
                    }
                    source = args[++i];
                    break;
                case "--dimster-only":
                    dimsterOnly = true;
                    break;
                case "--topics":
                    showTopics = true;
                    break;
                case "--group-offsets":
                    showGroupOffsets = true;
                    break;
                case "--group-members":
                    showGroupMembers = true;
                    break;
                case "--metadata-quorum":
                    showMetadataQuorum = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + a);
            }
        }
        if (source == null) {
            throw new IllegalArgumentException("--props is required");
        }
        Properties props = new Properties();
        if ("-".equals(source)) {
            // Don't try-with-resources on System.in — closing it would also
            // close the parent process's stdin in some shells.
            props.load(System.in);
        } else {
            try (InputStream in = Files.newInputStream(Path.of(source))) {
                props.load(in);
            }
        }
        return new ParsedArgs(props,
                new Filters(dimsterOnly, showTopics, showGroupOffsets, showGroupMembers, showMetadataQuorum));
    }

    /**
     * True if {@code topic} matches the dimster-created topic naming convention.
     * Mirrors the same prefix logic {@link KafkaCluster#deleteTestTopics()} uses
     * for cleanup — kept in sync via the public constant on {@link KafkaCluster}.
     */
    static boolean isDimsterTopic(String topic) {
        return topic != null && topic.startsWith(KafkaCluster.TOPIC_PREFIX);
    }

    /**
     * True if {@code groupId} matches the dimster-created group naming
     * convention (consumer or share). Same source of truth as
     * {@link KafkaCluster#deleteTestConsumerGroups()} /
     * {@link KafkaCluster#deleteTestShareGroups()}.
     */
    static boolean isDimsterGroup(String groupId) {
        return groupId != null && groupId.startsWith(KafkaCluster.TEST_GROUP_PREFIX);
    }

    private static void printSectionHeader(String title) {
        System.out.println(SECTION_RULE);
        System.out.println("  " + title);
        System.out.println(SECTION_RULE);
    }

    // ----------------------------------------------------------------------
    // TOPICS
    // ----------------------------------------------------------------------

    private static void printTopics(AdminClient admin, Filters filters) {
        printSectionHeader("TOPICS");
        Set<String> names;
        try {
            // listInternal(true) so __consumer_offsets, __share_group_state, and
            // __transaction_state are visible — these are the topics most worth
            // inspecting when debugging coordinator/share-group state, and the
            // AdminClient default of false silently hides them.
            names = admin.listTopics(new ListTopicsOptions().listInternal(true))
                    .names()
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            System.out.println("(error listing topics: " + ex.getMessage() + ")");
            return;
        }
        if (filters.dimsterOnly()) {
            // Filter before sorting/batching so we don't pay describe RPCs on
            // topics the user has explicitly opted out of seeing.
            names = filterDimsterTopics(names);
        }
        if (names.isEmpty()) {
            System.out.println("(none)");
            return;
        }

        List<String> sorted = new ArrayList<>(names);
        Collections.sort(sorted);

        if (sorted.size() > LARGE_TOPIC_THRESHOLD) {
            System.out.println("(describing " + sorted.size() + " topics, may take a moment...)");
        }

        // Batch to keep individual describeTopics / describeConfigs RPCs sane.
        for (int i = 0; i < sorted.size(); i += DESCRIBE_BATCH_SIZE) {
            int end = Math.min(i + DESCRIBE_BATCH_SIZE, sorted.size());
            List<String> chunk = sorted.subList(i, end);
            describeAndPrintTopicChunk(admin, chunk);
        }
    }

    private static Set<String> filterDimsterTopics(Set<String> names) {
        Set<String> filtered = new HashSet<>();
        for (String n : names) {
            if (isDimsterTopic(n)) filtered.add(n);
        }
        return filtered;
    }

    private static void describeAndPrintTopicChunk(AdminClient admin, List<String> chunk) {
        Map<String, TopicDescription> descs;
        try {
            descs = admin.describeTopics(chunk)
                    .allTopicNames()
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            System.out.println("(error describing topics " + chunk.get(0) + "..: " + ex.getMessage() + ")");
            return;
        }

        // Topic-level configs are a separate RPC. Best-effort: if it fails we
        // still print partition info, just without a Configs column body.
        Map<ConfigResource, Config> configs;
        try {
            List<ConfigResource> resources = new ArrayList<>(chunk.size());
            for (String t : chunk) {
                resources.add(new ConfigResource(ConfigResource.Type.TOPIC, t));
            }
            configs = admin.describeConfigs(resources)
                    .all()
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            configs = Map.of();
        }

        for (String topic : chunk) {
            TopicDescription d = descs.get(topic);
            if (d == null) continue;
            int partitions = d.partitions().size();
            int rf = partitions > 0 ? d.partitions().get(0).replicas().size() : 0;
            String configsStr = formatConfigs(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic)));
            System.out.printf("Topic: %s\tPartitionCount: %d\tReplicationFactor: %d\tConfigs: %s%n",
                    topic, partitions, rf, configsStr);
            for (TopicPartitionInfo p : d.partitions()) {
                System.out.printf("\tTopic: %s\tPartition: %d\tLeader: %s\tReplicas: %s\tIsr: %s%n",
                        topic,
                        p.partition(),
                        p.leader() != null ? String.valueOf(p.leader().id()) : "none",
                        joinNodeIds(p.replicas()),
                        joinNodeIds(p.isr()));
            }
        }
    }

    /**
     * Formats the topic config entries as {@code k=v,k=v,...}. Only non-default
     * entries are shown — most topics inherit dozens of broker defaults that
     * would drown out the actual overrides.
     */
    private static String formatConfigs(Config cfg) {
        if (cfg == null) {
            return "";
        }
        List<String> kvs = new ArrayList<>();
        for (ConfigEntry e : cfg.entries()) {
            if (!e.isDefault()) {
                kvs.add(e.name() + "=" + e.value());
            }
        }
        return String.join(",", kvs);
    }

    private static String joinNodeIds(List<Node> nodes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(nodes.get(i).id());
        }
        return sb.toString();
    }

    // ----------------------------------------------------------------------
    // CONSUMER GROUPS
    // ----------------------------------------------------------------------

    private static void printConsumerGroups(AdminClient admin, RawAdmin raw, Filters filters) {
        printSectionHeader("CONSUMER GROUPS");
        Set<String> groups;
        try {
            // RawAdmin already does the right thing across versions: on 4.x it
            // queries GroupType.CONSUMER_GROUP + CONSUMER together, on 3.x it falls
            // back to listConsumerGroups.
            groups = raw.listGroupIds(GroupKind.CLASSIC_OR_CONSUMER);
        } catch (Exception ex) {
            System.out.println("(error listing consumer groups: " + ex.getMessage() + ")");
            return;
        }
        if (filters.dimsterOnly()) {
            groups = filterDimsterGroups(groups);
        }
        if (groups.isEmpty()) {
            System.out.println("(none)");
            return;
        }

        List<String> sorted = new ArrayList<>(groups);
        Collections.sort(sorted);
        for (String g : sorted) {
            System.out.println();
            System.out.println("--- Group: " + g + " ---");
            describeOneConsumerGroup(admin, raw, g, filters);
        }
    }

    private static Set<String> filterDimsterGroups(Set<String> groups) {
        Set<String> filtered = new HashSet<>();
        for (String g : groups) {
            if (isDimsterGroup(g)) filtered.add(g);
        }
        return filtered;
    }

    private static void describeOneConsumerGroup(AdminClient admin, RawAdmin raw, String groupId, Filters filters) {
        ConsumerGroupDescription desc;
        try {
            desc = admin.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            System.out.println("(error describing group: " + ex.getMessage() + ")");
            return;
        }

        // Header line is always printed when the group block renders, regardless
        // of which subsection flags are active — it's metadata about the group
        // itself, not about offsets or members specifically.
        System.out.printf("State: %s\tCoordinator: %s\tMembers: %d\tAssignor: %s%n",
                desc.state(),
                desc.coordinator() != null ? String.valueOf(desc.coordinator().id()) : "unknown",
                desc.members().size(),
                desc.partitionAssignor());

        if (filters.offsetsSubsectionRequested()) {
            printGroupOffsets(groupId, raw);
        }
        if (filters.membersSubsectionRequested()) {
            printGroupMembers(desc);
        }
    }

    private static void printGroupOffsets(String groupId, RawAdmin raw) {
        Map<TopicPartition, OffsetAndMetadata> committed;
        try {
            committed = raw.committedOffsets(groupId);
        } catch (Exception ex) {
            System.out.println("OFFSETS: (error: " + ex.getMessage() + ")");
            return;
        }
        if (committed.isEmpty()) {
            System.out.println("OFFSETS: (no committed offsets)");
            return;
        }
        Map<TopicPartition, Long> ends;
        try {
            ends = raw.endOffsets(committed.keySet());
        } catch (Exception ex) {
            // Lag column will read "-" for every row.
            ends = Map.of();
        }
        System.out.println("OFFSETS:");
        System.out.printf("  %-40s %-10s %-15s %-15s %-10s%n",
                "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG");

        // Sort for deterministic, scannable output.
        List<Map.Entry<TopicPartition, OffsetAndMetadata>> entries =
                new ArrayList<>(committed.entrySet());
        entries.sort((a, b) -> {
            int t = a.getKey().topic().compareTo(b.getKey().topic());
            return t != 0 ? t : Integer.compare(a.getKey().partition(), b.getKey().partition());
        });
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : entries) {
            TopicPartition tp = e.getKey();
            long current = e.getValue().offset();
            Long endOffset = ends.get(tp);
            String endStr = endOffset != null ? String.valueOf(endOffset) : "-";
            String lagStr = endOffset != null ? String.valueOf(endOffset - current) : "-";
            System.out.printf("  %-40s %-10d %-15d %-15s %-10s%n",
                    tp.topic(), tp.partition(), current, endStr, lagStr);
        }
    }

    private static void printGroupMembers(ConsumerGroupDescription desc) {
        if (desc.members().isEmpty()) {
            System.out.println("MEMBERS: (none)");
            return;
        }
        System.out.println("MEMBERS:");
        System.out.printf("  %-50s %-25s %-30s %s%n",
                "CONSUMER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
        for (MemberDescription m : desc.members()) {
            String assignment = m.assignment() != null ? formatAssignment(m.assignment()) : "";
            System.out.printf("  %-50s %-25s %-30s %s%n",
                    m.consumerId(),
                    m.host(),
                    m.clientId(),
                    assignment);
        }
    }

    /** Adapter: pull partitions out of a MemberAssignment then format. */
    private static String formatAssignment(MemberAssignment assignment) {
        return formatAssignment(assignment.topicPartitions());
    }

    /**
     * Renders a member's partition assignment compactly. Two cases:
     * <ul>
     *   <li><b>Single topic</b> (the common case — most groups subscribe to
     *       one topic): just the partition numbers, e.g. {@code 0,3,6}.
     *       Topic name is implicit from the OFFSETS section above.</li>
     *   <li><b>Multiple topics</b>: keep the topic prefix so partitions
     *       remain disambiguatable, e.g. {@code orders(0,3,6),
     *       payments(1,2)}.</li>
     * </ul>
     * Used by both the consumer-group and share-group printers.
     */
    private static String formatAssignment(Set<TopicPartition> partitions) {
        Map<String, List<Integer>> byTopic = new TreeMap<>();
        for (TopicPartition tp : partitions) {
            byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
        }
        if (byTopic.isEmpty()) {
            return "";
        }
        if (byTopic.size() == 1) {
            List<Integer> ps = byTopic.values().iterator().next();
            Collections.sort(ps);
            return joinInts(ps);
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            Collections.sort(e.getValue());
            sb.append(e.getKey()).append("(").append(joinInts(e.getValue())).append(")");
        }
        return sb.toString();
    }

    private static String joinInts(List<Integer> ints) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ints.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(ints.get(i));
        }
        return sb.toString();
    }

    // ----------------------------------------------------------------------
    // SHARE GROUPS
    // ----------------------------------------------------------------------

    private static void printShareGroups(RawAdmin raw, Capabilities caps, Filters filters) {
        printSectionHeader("SHARE GROUPS");
        if (!caps.shareGroupsSupported()) {
            // Match the spirit of the full-mode "(kafka-share-groups.sh not
            // available)" line — explain why no data is shown.
            System.out.println("(share groups not supported on Kafka client " + caps.kafkaClientVersion() + ")");
            return;
        }

        Set<String> groups;
        try {
            groups = raw.listGroupIds(GroupKind.SHARE);
        } catch (UnsupportedOperationException ex) {
            // Belt-and-braces: if some impl reports shareGroupsSupported=true
            // but throws on listGroupIds(SHARE), don't crash the whole tool.
            System.out.println("(share groups not supported by SPI impl)");
            return;
        } catch (Exception ex) {
            System.out.println("(error listing share groups: " + ex.getMessage() + ")");
            return;
        }
        if (filters.dimsterOnly()) {
            groups = filterDimsterGroups(groups);
        }
        if (groups.isEmpty()) {
            System.out.println("(none)");
            return;
        }

        List<String> sorted = new ArrayList<>(groups);
        Collections.sort(sorted);
        for (String g : sorted) {
            System.out.println();
            System.out.println("--- Share Group: " + g + " ---");
            describeOneShareGroup(raw, g, filters);
        }
    }

    /**
     * Print state, coordinator, members, and per-partition lag for a single
     * share group. The describe and lag calls are independent so a failure
     * in one does not skip the other — a half-rendered group is still more
     * useful than `(error)` with no detail.
     */
    private static void describeOneShareGroup(RawAdmin raw, String groupId, Filters filters) {
        // 1. State + members via describeShareGroup. Available on 4.1+ — the
        //    outer printShareGroups already gated on capabilities, so this
        //    should succeed; we still catch UnsupportedOperationException as
        //    a backstop for SPI impls that report supported but throw.
        //
        //    Note: we call describeShareGroup even when only --group-offsets
        //    is set, because the per-group header line uses info.state(),
        //    info.coordinatorId() and info.members().size() — and the user
        //    confirmed they always want that header.
        ShareGroupInfo info = null;
        try {
            info = raw.describeShareGroup(groupId);
        } catch (UnsupportedOperationException ex) {
            System.out.println("(describe not supported by SPI impl)");
        } catch (Exception ex) {
            System.out.println("(error describing group: " + ex.getMessage() + ")");
        }

        if (info != null) {
            System.out.printf("State: %s\tCoordinator: %s\tMembers: %d%n",
                    info.state(),
                    info.coordinatorId() >= 0 ? String.valueOf(info.coordinatorId()) : "unknown",
                    info.members().size());
        }

        // 2. Per-partition start offset + lag via shareGroupOffsets.
        //    Independent from the describe call — an EMPTY group has no
        //    members but may still have committed (and therefore laggy)
        //    partitions worth showing. Printed before MEMBERS because
        //    offsets are the operational signal most users scan first.
        if (filters.offsetsSubsectionRequested()) {
            printShareGroupOffsets(raw, groupId);
        }

        if (info != null && filters.membersSubsectionRequested()) {
            printShareGroupMembers(info.members());
        }
    }

    private static void printShareGroupMembers(List<ShareGroupMember> members) {
        if (members.isEmpty()) {
            System.out.println("MEMBERS: (none)");
            return;
        }
        // Compute column widths from the actual data so a 36-char broker-issued
        // member UUID doesn't pad an 8-char client.id with 28 spaces (or the
        // other way around). Two-space gap between columns reads as deliberate
        // separation without being wasteful.
        int memberIdW = "MEMBER-ID".length();
        int hostW = "HOST".length();
        int clientIdW = "CLIENT-ID".length();
        for (ShareGroupMember m : members) {
            memberIdW = Math.max(memberIdW, m.consumerId() != null ? m.consumerId().length() : 0);
            hostW = Math.max(hostW, m.host() != null ? m.host().length() : 0);
            clientIdW = Math.max(clientIdW, m.clientId() != null ? m.clientId().length() : 0);
        }
        String fmt = "  %-" + memberIdW + "s  %-" + hostW + "s  %-" + clientIdW + "s  %s%n";
        System.out.println("MEMBERS:");
        System.out.printf(fmt, "MEMBER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
        for (ShareGroupMember m : members) {
            System.out.printf(fmt,
                    m.consumerId(),
                    m.host() != null ? m.host() : "",
                    m.clientId() != null ? m.clientId() : "",
                    formatAssignment(m.assignment()));
        }
    }

    /**
     * Per-partition share-partition start offset (SPSO) and lag for a share
     * group, via {@link RawAdmin#shareGroupOffsets}. On 4.2+ both come from
     * the broker in one RPC; on 4.1 the impl computes lag client-side.
     */
    private static void printShareGroupOffsets(RawAdmin raw, String groupId) {
        Map<TopicPartition, ShareGroupOffsetInfo> offsets;
        try {
            offsets = raw.shareGroupOffsets(groupId);
        } catch (UnsupportedOperationException ex) {
            System.out.println("OFFSETS: (not supported by SPI impl)");
            return;
        } catch (Exception ex) {
            System.out.println("OFFSETS: (error: " + ex.getMessage() + ")");
            return;
        }
        if (offsets.isEmpty()) {
            System.out.println("OFFSETS: (no offset data)");
            return;
        }
        System.out.println("OFFSETS:");
        System.out.printf("  %-40s %-10s %-15s %-15s%n",
                "TOPIC", "PARTITION", "START-OFFSET", "LAG");

        List<Map.Entry<TopicPartition, ShareGroupOffsetInfo>> entries =
                new ArrayList<>(offsets.entrySet());
        entries.sort((a, b) -> {
            int t = a.getKey().topic().compareTo(b.getKey().topic());
            return t != 0 ? t : Integer.compare(a.getKey().partition(), b.getKey().partition());
        });
        for (Map.Entry<TopicPartition, ShareGroupOffsetInfo> e : entries) {
            System.out.printf("  %-40s %-10d %-15d %-15d%n",
                    e.getKey().topic(),
                    e.getKey().partition(),
                    e.getValue().startOffset(),
                    e.getValue().lag());
        }
    }

    // ----------------------------------------------------------------------
    // METADATA QUORUM (KRaft)
    // ----------------------------------------------------------------------

    /**
     * Print the KRaft metadata quorum: leader id/epoch, high-water mark, and
     * per-replica state for both voters and observers. Goes through
     * {@link AdminClient#describeMetadataQuorum()} which is KRaft-only — on a
     * ZooKeeper-backed cluster the call fails and we render a single
     * {@code (error: ...)} line so the rest of the inspect still completes.
     *
     * <p>No filtering of voters/observers — the quorum is small (typically 3-5
     * controllers + a handful of broker observers) and seeing the whole list
     * is the point. {@code --dimster-only} doesn't apply here.
     */
    private static void printMetadataQuorum(AdminClient admin) {
        printSectionHeader("METADATA QUORUM");
        QuorumInfo info;
        try {
            info = admin.describeMetadataQuorum()
                    .quorumInfo()
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            // KRaft is required — on a ZK cluster this fails with
            // UnsupportedVersionException; on a misconfigured one with various
            // RPC errors. Both surface as the same "(error: ...)" line.
            System.out.println("(error describing metadata quorum: " + ex.getMessage() + ")");
            return;
        }

        System.out.printf("LeaderId: %d\tLeaderEpoch: %d\tHighWatermark: %d%n",
                info.leaderId(), info.leaderEpoch(), info.highWatermark());

        printQuorumReplicas("VOTERS", info.voters());
        printQuorumReplicas("OBSERVERS", info.observers());
    }

    /**
     * Render a list of quorum replicas (voters or observers) as an aligned
     * table. Empty lists print a {@code (none)} line so the section structure
     * is consistent regardless of cluster topology (a 3-controller cluster has
     * voters but no observers, for example).
     */
    private static void printQuorumReplicas(String label, List<QuorumInfo.ReplicaState> replicas) {
        if (replicas.isEmpty()) {
            System.out.println(label + ": (none)");
            return;
        }
        System.out.println(label + ":");
        System.out.printf("  %-12s %-15s %-22s %-22s%n",
                "REPLICA-ID", "LOG-END-OFFSET", "LAST-FETCH-TS-MS", "LAST-CAUGHT-UP-TS-MS");
        for (QuorumInfo.ReplicaState r : replicas) {
            System.out.printf("  %-12d %-15d %-22s %-22s%n",
                    r.replicaId(),
                    r.logEndOffset(),
                    formatOptionalLong(r.lastFetchTimestamp()),
                    formatOptionalLong(r.lastCaughtUpTimestamp()));
        }
    }

    /** Render an OptionalLong as its value or "-" when absent. Same pattern the OFFSETS columns use. */
    private static String formatOptionalLong(OptionalLong v) {
        return v.isPresent() ? String.valueOf(v.getAsLong()) : "-";
    }
}
