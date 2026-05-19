package dev.dimster.tools;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for InspectMain's argument parsing and filter resolution.
 *
 * <p>The actual print methods are not exercised here — they shell out to
 * AdminClient + RawAdmin which need a running Kafka. Coverage of those paths
 * is end-to-end via `dimster inspect` against a deployed env. What we CAN
 * pin down without a broker is:
 * <ul>
 *   <li>arg parsing recognizes every supported flag and rejects bad input,</li>
 *   <li>the {@link InspectMain.Filters} truth table is correct (this is the
 *       logic main() relies on to decide which sections render),</li>
 *   <li>the dimster-resource predicates correctly identify dimster-created
 *       topics and groups vs. internal/user-created ones.</li>
 * </ul>
 */
public class InspectMainTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File propsFile;

    @Before
    public void setUp() throws IOException {
        // parseArgs needs to actually load a Properties file when --props points
        // at a path. The contents are irrelevant for these tests — we only care
        // that load() succeeds and the resulting Properties round-trips.
        propsFile = tempFolder.newFile("kafka.properties");
        try (FileWriter w = new FileWriter(propsFile)) {
            w.write("bootstrap.servers=localhost:9092\n");
        }
    }

    /** Convenience: build an argv with --props plus the test-supplied flags. */
    private InspectMain.ParsedArgs parse(String... extraArgs) throws IOException {
        String[] full = new String[extraArgs.length + 2];
        full[0] = "--props";
        full[1] = propsFile.getAbsolutePath();
        System.arraycopy(extraArgs, 0, full, 2, extraArgs.length);
        return InspectMain.parseArgs(full);
    }

    // --------------------------------------------------------------------
    // parseArgs
    // --------------------------------------------------------------------

    @Test
    public void parseArgsNoFlagsLeavesAllFiltersOff() throws IOException {
        InspectMain.ParsedArgs parsed = parse();
        InspectMain.Filters f = parsed.filters();
        assertFalse(f.dimsterOnly());
        assertFalse(f.showTopics());
        assertFalse(f.showGroupOffsets());
        assertFalse(f.showGroupMembers());
        assertFalse(f.showMetadataQuorum());
        assertEquals("localhost:9092", parsed.props().getProperty("bootstrap.servers"));
    }

    @Test
    public void parseArgsRecognizesDimsterOnly() throws IOException {
        InspectMain.Filters f = parse("--dimster-only").filters();
        assertTrue(f.dimsterOnly());
        assertFalse(f.showTopics());
        assertFalse(f.showGroupOffsets());
        assertFalse(f.showGroupMembers());
    }

    @Test
    public void parseArgsRecognizesTopics() throws IOException {
        InspectMain.Filters f = parse("--topics").filters();
        assertTrue(f.showTopics());
        assertFalse(f.dimsterOnly());
    }

    @Test
    public void parseArgsRecognizesGroupOffsets() throws IOException {
        InspectMain.Filters f = parse("--group-offsets").filters();
        assertTrue(f.showGroupOffsets());
        assertFalse(f.showGroupMembers());
    }

    @Test
    public void parseArgsRecognizesGroupMembers() throws IOException {
        InspectMain.Filters f = parse("--group-members").filters();
        assertTrue(f.showGroupMembers());
        assertFalse(f.showGroupOffsets());
    }

    @Test
    public void parseArgsRecognizesMetadataQuorum() throws IOException {
        InspectMain.Filters f = parse("--metadata-quorum").filters();
        assertTrue(f.showMetadataQuorum());
        assertFalse(f.showTopics());
    }

    @Test
    public void parseArgsAcceptsAllFlagsCombined() throws IOException {
        InspectMain.Filters f = parse(
                "--dimster-only", "--topics", "--group-offsets", "--group-members", "--metadata-quorum"
        ).filters();
        assertTrue(f.dimsterOnly());
        assertTrue(f.showTopics());
        assertTrue(f.showGroupOffsets());
        assertTrue(f.showGroupMembers());
        assertTrue(f.showMetadataQuorum());
    }

    @Test
    public void parseArgsAcceptsFlagsBeforeProps() throws IOException {
        // Flag order shouldn't matter — the Rust CLI is free to emit them in
        // any order it likes.
        InspectMain.ParsedArgs parsed = InspectMain.parseArgs(new String[]{
                "--dimster-only", "--props", propsFile.getAbsolutePath(), "--topics"
        });
        assertTrue(parsed.filters().dimsterOnly());
        assertTrue(parsed.filters().showTopics());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseArgsRejectsUnknownFlag() throws IOException {
        parse("--bogus");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseArgsRejectsMissingProps() throws IOException {
        InspectMain.parseArgs(new String[]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseArgsRejectsPropsWithoutValue() throws IOException {
        // --props is the last token: index+1 is out of range. Should throw,
        // not crash with an ArrayIndexOutOfBoundsException.
        InspectMain.parseArgs(new String[]{"--props"});
    }

    // --------------------------------------------------------------------
    // Filters truth table — this is the single source of truth for which
    // sections render. Every combination is worth pinning down because main()
    // depends on these accessors.
    // --------------------------------------------------------------------

    @Test
    public void filtersNoFlagsRendersEverything() {
        InspectMain.Filters f = new InspectMain.Filters(false, false, false, false, false);
        assertTrue("no flags = all sections rendered", f.noSectionFlagsSet());
        assertTrue(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertTrue(f.membersSubsectionRequested());
        assertTrue(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersTopicsOnlyOmitsAllOtherSections() {
        InspectMain.Filters f = new InspectMain.Filters(false, true, false, false, false);
        assertTrue(f.topicsSectionRequested());
        assertFalse(f.groupSectionsRequested());
        assertFalse(f.offsetsSubsectionRequested());
        assertFalse(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersGroupOffsetsOnlyOmitsTopicsMembersAndQuorum() {
        InspectMain.Filters f = new InspectMain.Filters(false, false, true, false, false);
        assertFalse(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertFalse(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersGroupMembersOnlyOmitsTopicsOffsetsAndQuorum() {
        InspectMain.Filters f = new InspectMain.Filters(false, false, false, true, false);
        assertFalse(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertFalse(f.offsetsSubsectionRequested());
        assertTrue(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersMetadataQuorumOnlyOmitsTopicsAndGroups() {
        InspectMain.Filters f = new InspectMain.Filters(false, false, false, false, true);
        assertFalse(f.topicsSectionRequested());
        assertFalse(f.groupSectionsRequested());
        assertFalse(f.offsetsSubsectionRequested());
        assertFalse(f.membersSubsectionRequested());
        assertTrue(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersTopicsPlusGroupOffsetsShowsBothButNotMembersOrQuorum() {
        InspectMain.Filters f = new InspectMain.Filters(false, true, true, false, false);
        assertTrue(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertFalse(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersBothSubsectionsButNotTopicsOrQuorum() {
        InspectMain.Filters f = new InspectMain.Filters(false, false, true, true, false);
        assertFalse(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertTrue(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersTopicsPlusMetadataQuorumOmitsGroups() {
        InspectMain.Filters f = new InspectMain.Filters(false, true, false, false, true);
        assertTrue(f.topicsSectionRequested());
        assertFalse(f.groupSectionsRequested());
        assertTrue(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersDimsterOnlyDoesNotAffectSectionSelection() {
        // dimsterOnly is on the resource-filter axis, not the section axis.
        // With no section flags it should still render everything.
        InspectMain.Filters f = new InspectMain.Filters(true, false, false, false, false);
        assertTrue(f.dimsterOnly());
        assertTrue(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertTrue(f.membersSubsectionRequested());
        assertTrue(f.metadataQuorumSectionRequested());
    }

    @Test
    public void filtersDimsterOnlyComposesWithSectionFlags() {
        InspectMain.Filters f = new InspectMain.Filters(true, false, true, false, false);
        assertTrue(f.dimsterOnly());
        assertFalse(f.topicsSectionRequested());
        assertTrue(f.groupSectionsRequested());
        assertTrue(f.offsetsSubsectionRequested());
        assertFalse(f.membersSubsectionRequested());
        assertFalse(f.metadataQuorumSectionRequested());
    }

    // --------------------------------------------------------------------
    // Resource predicates — match the same prefix rules KafkaCluster uses
    // for cleanup, so dimster's create/inspect/delete views stay consistent.
    // --------------------------------------------------------------------

    @Test
    public void isDimsterTopicAcceptsKnownPatterns() {
        assertTrue(InspectMain.isDimsterTopic("dimster-tpc-1"));
        assertTrue(InspectMain.isDimsterTopic("dimster-avail-probe"));
        // Bare prefix also passes — KafkaCluster's deleteTestTopics uses the
        // same startsWith check, so both views agree.
        assertTrue(InspectMain.isDimsterTopic("dimster"));
    }

    @Test
    public void isDimsterTopicRejectsInternalAndUserTopics() {
        assertFalse(InspectMain.isDimsterTopic("__consumer_offsets"));
        assertFalse(InspectMain.isDimsterTopic("__share_group_state"));
        assertFalse(InspectMain.isDimsterTopic("__transaction_state"));
        assertFalse(InspectMain.isDimsterTopic("orders"));
        assertFalse(InspectMain.isDimsterTopic(""));
        assertFalse(InspectMain.isDimsterTopic(null));
    }

    @Test
    public void isDimsterGroupAcceptsTestGroupPrefix() {
        assertTrue(InspectMain.isDimsterGroup("dimster-grp-abc123"));
        assertTrue(InspectMain.isDimsterGroup("dimster-grp-"));
    }

    @Test
    public void isDimsterGroupRejectsTopicPrefixOrUserGroups() {
        // Group prefix is stricter than the topic prefix — "dimster" alone
        // is a topic but not a dimster group.
        assertFalse(InspectMain.isDimsterGroup("dimster"));
        assertFalse(InspectMain.isDimsterGroup("dimster-tpc-1"));
        assertFalse(InspectMain.isDimsterGroup("my-group"));
        assertFalse(InspectMain.isDimsterGroup(""));
        assertFalse(InspectMain.isDimsterGroup(null));
    }
}
