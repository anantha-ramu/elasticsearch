/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.LatestCommit;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Measured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.NotMeasured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Reason;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Result;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.ShardCopy;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for {@link ShardBaselineCache}. Loads run on a {@link DeterministicTaskQueue}, so each test decides when they run. The
 * repository and the shards are fakes ({@link FakeRepository}, {@link FakeShard}): the cache only asks them for backup metadata and the
 * latest commit, and the fakes record what was asked, and when, in {@link #events}. Time only moves when a test moves it.
 */
public class ShardBaselineCacheTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");
    private static final IndexId INDEX_ID = new IndexId("index", "index-uuid");
    private static final ShardGeneration GENERATION_1 = new ShardGeneration("generation-1");
    private static final ShardGeneration GENERATION_2 = new ShardGeneration("generation-2");

    // What the fakes were asked, in order: "read <generation>" for the backup metadata, "commit" for the latest commit.
    private final List<String> events = new ArrayList<>();
    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final FakeRepository repository = new FakeRepository();
    private long nowMillis = randomLongBetween(0, 1_000_000);

    public void testAShardIsMeasuredOnceItsBaselineIsLoaded() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        final var cfs0 = file("_0.cfs", 1000);
        final var cfs1 = file("_1.cfs", 2000);
        repository.metadata.put(GENERATION_1, backedUp(cfs0));
        final var commit = new FakeShard(randomBoolean() ? "state-2" : null, cfs0, cfs1);

        assertThat(measure(cache, shard, GENERATION_1, commit), equalTo(new NotMeasured(Reason.LOADING)));
        taskQueue.runAllRunnableTasks();

        // The same answer as the baseline gives when used directly.
        final long expected = ShardBaseline.of(backedUp(cfs0), commit.files).bytesToUpload(commit.stateId, commit.files);
        assertThat(expected, equalTo(2000L));
        assertThat(measure(cache, shard, GENERATION_1, commit), equalTo(new Measured(GENERATION_1, expected)));
        assertThat(cache.ramBytesUsed(), greaterThan(0L));
        // The load took the commit and then read the metadata straight into a baseline for its files. The measure took it again.
        assertThat(events, equalTo(List.of("commit", "read " + GENERATION_1, "commit")));
    }

    public void testANewShardGenerationNeedsNoRead() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        final var commit = new FakeShard(null, file("_0.cfs", 1000));

        measure(cache, shard, ShardGenerations.NEW_SHARD_GEN, commit);
        taskQueue.runAllRunnableTasks();

        // The repository has nothing for the shard yet, so every file counts, and the repository isn't read.
        assertThat(
            measure(cache, shard, ShardGenerations.NEW_SHARD_GEN, commit),
            equalTo(new Measured(ShardGenerations.NEW_SHARD_GEN, 1000))
        );
        assertThat(events, equalTo(List.of("commit", "commit")));
    }

    public void testLoadsAreThrottled() {
        // The cache hands a load to the executor only when one of its running loads has finished.
        final var handedOut = new int[1];
        final Executor countingExecutor = task -> {
            handedOut[0] += 1;
            taskQueue.scheduleNow(task);
        };
        final var cache = new ShardBaselineCache(repository, countingExecutor, () -> nowMillis, Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final int shards = between(ShardBaselineCache.MAX_CONCURRENT_LOADS + 1, 20);
        for (int i = 0; i < shards; i++) {
            measure(cache, shard(i), GENERATION_1, new FakeShard(null));
        }

        assertThat(handedOut[0], equalTo(ShardBaselineCache.MAX_CONCURRENT_LOADS));
        taskQueue.runAllRunnableTasks();

        assertThat(handedOut[0], equalTo(shards));
        for (int i = 0; i < shards; i++) {
            assertThat(measure(cache, shard(i), GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
        }
    }

    public void testAShardHasOneLoadAtATime() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final var shard = shard(0);

        for (int i = between(2, 5); i > 0; i--) {
            assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
        }
        taskQueue.runAllRunnableTasks();

        assertThat(repository.reads, equalTo(List.of(GENERATION_1)));
    }

    public void testANewGenerationIsNotMeasuredUntilItsBaselineIsBuilt() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        final var cfs0 = file("_0.cfs", 1000);
        final var commit = new FakeShard(null, cfs0);
        repository.metadata.put(GENERATION_1, backedUp(cfs0));
        repository.metadata.put(GENERATION_2, backedUp());
        measure(cache, shard, GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard, GENERATION_1, commit), equalTo(new Measured(GENERATION_1, 0)));

        // The metadata changed, here because a deletion removed _0.cfs. The old baseline would count 0, too little.
        assertThat(measure(cache, shard, GENERATION_2, commit), equalTo(new NotMeasured(Reason.LOADING)));
        assertThat(cache.ramBytesUsed(), equalTo(0L));
        taskQueue.runAllRunnableTasks();

        assertThat(measure(cache, shard, GENERATION_2, commit), equalTo(new Measured(GENERATION_2, 1000)));
    }

    public void testAQueuedLoadReadsTheNewestGeneration() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.metadata.put(GENERATION_2, backedUp());

        measure(cache, shard, GENERATION_1, new FakeShard(null));
        measure(cache, shard, GENERATION_2, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        // The load queued for generation-1 hadn't started when generation-2 came, so it read generation-2 instead.
        assertThat(repository.reads, equalTo(List.of(GENERATION_2)));
        assertThat(measure(cache, shard, GENERATION_2, new FakeShard(null)), equalTo(new Measured(GENERATION_2, 0)));
    }

    public void testALoadThatFinishesForAnOldGenerationLoadsTheNewOne() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.metadata.put(GENERATION_1, backedUp());
        repository.metadata.put(GENERATION_2, backedUp());
        // While generation-1 is being read, a snapshot finishes and the next measure asks about generation-2.
        repository.duringRead = generation -> {
            if (generation.equals(GENERATION_1)) {
                assertThat(measure(cache, shard, GENERATION_2, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
            }
        };

        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        // The baseline for generation-1 was thrown away, and generation-2 was loaded straight away, without waiting for a measure.
        assertThat(repository.reads, equalTo(List.of(GENERATION_1, GENERATION_2)));
        assertThat(measure(cache, shard, GENERATION_2, new FakeShard(null)), equalTo(new Measured(GENERATION_2, 0)));
    }

    public void testFailedLoadsAreRetriedLessAndLessOften() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.failure = randomFrom(new IOException("simulated"), new NoSuchFileException("index-generation-1"));
        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        long expectedDelay = ShardBaselineCache.FIRST_RETRY_DELAY.millis();
        for (int failures = 1; failures <= 8; failures++) {
            assertThat(repository.reads.size(), equalTo(failures));
            // Not tried again before the delay.
            nowMillis += expectedDelay - 1;
            assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.READ_FAILED)));
            assertFalse(taskQueue.hasRunnableTasks());
            // Tried again on the first measure after it, and loading meanwhile.
            nowMillis += 1;
            assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
            taskQueue.runAllRunnableTasks();
            expectedDelay = Math.min(2 * expectedDelay, ShardBaselineCache.MAX_RETRY_DELAY.millis());
        }
        assertThat(expectedDelay, equalTo(ShardBaselineCache.MAX_RETRY_DELAY.millis()));

        // Once the repository can be read again, the next retry builds the baseline.
        repository.failure = null;
        repository.metadata.put(GENERATION_1, backedUp());
        nowMillis += expectedDelay;
        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
    }

    public void testANewGenerationIsLoadedAtOnceAfterAFailure() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.failure = new NoSuchFileException("index-generation-1");
        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.READ_FAILED)));

        // The blob was gone because generation-2 had replaced it. The new generation doesn't wait for the delay.
        repository.failure = null;
        repository.metadata.put(GENERATION_2, backedUp());
        assertThat(measure(cache, shard, GENERATION_2, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
        taskQueue.runAllRunnableTasks();

        assertThat(measure(cache, shard, GENERATION_2, new FakeShard(null)), equalTo(new Measured(GENERATION_2, 0)));
    }

    public void testFailuresAreWarnedAboutAtMostOnceEveryTenMinutes() {
        final var cache = cache(Long.MAX_VALUE);
        repository.failure = new IOException("simulated");
        final var logger = ShardBaselineCache.class.getCanonicalName();

        // The first failure is warned about at once.
        MockLog.assertThatLogger(() -> {
            measure(cache, shard(0), GENERATION_1, new FakeShard(null));
            taskQueue.runAllRunnableTasks();
        },
            ShardBaselineCache.class,
            new MockLog.SeenEventExpectation(
                "first failure",
                logger,
                Level.WARN,
                "[*repo*] failed to load the backup metadata of 1 shard(s) since the last warning; *[simulated]"
            )
        );
        // Other failures of the same repository within ten minutes are not.
        MockLog.assertThatLogger(() -> {
            nowMillis += ShardBaselineCache.WARNING_INTERVAL.millis() - 1;
            measure(cache, shard(1), GENERATION_1, new FakeShard(null));
            measure(cache, shard(2), GENERATION_1, new FakeShard(null));
            taskQueue.runAllRunnableTasks();
        }, ShardBaselineCache.class, new MockLog.UnseenEventExpectation("no warning", logger, Level.WARN, "*"));
        // The next failure after that is, and says how many there were.
        MockLog.assertThatLogger(() -> {
            nowMillis += 1;
            measure(cache, shard(3), GENERATION_1, new FakeShard(null));
            taskQueue.runAllRunnableTasks();
        },
            ShardBaselineCache.class,
            new MockLog.SeenEventExpectation(
                "the failures since",
                logger,
                Level.WARN,
                "[*repo*] failed to load the backup metadata of 3 shard(s) since the last warning; *"
            )
        );
    }

    public void testACommitThatCannotBeTakenDuringALoadIsNotWarnedAbout() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.metadata.put(GENERATION_1, backedUp());
        // For example, the shard is closing while its baseline loads. The repository is fine, so there is no warning about it.
        final var closing = new FakeShard(null);
        closing.failure = new IOException("simulated");

        MockLog.assertThatLogger(() -> {
            measure(cache, shard, GENERATION_1, closing);
            taskQueue.runAllRunnableTasks();
        },
            ShardBaselineCache.class,
            new MockLog.UnseenEventExpectation("no warning", ShardBaselineCache.class.getCanonicalName(), Level.WARN, "*")
        );
        assertThat(measure(cache, shard, GENERATION_1, closing), equalTo(new NotMeasured(Reason.READ_FAILED)));
        // The commit is taken first, so the repository isn't read for nothing.
        assertThat(repository.reads, equalTo(List.of()));
    }

    public void testTheBudgetRefusesNewShardsButNeverARebuild() {
        final var cfs0 = file("_0.cfs", 1000);
        final var commit = new FakeShard(null, cfs0);
        repository.metadata.put(GENERATION_1, backedUp(cfs0));
        final long oneBaseline = ShardBaseline.of(backedUp(cfs0), commit.files).ramBytesUsed();
        // Room for one baseline, and not for two. The budget isn't full after the first, so the second is read and then refused.
        final long budget = oneBaseline + 1;
        final var cache = cache(budget);
        measure(cache, shard(0), GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard(0), GENERATION_1, commit), equalTo(new Measured(GENERATION_1, 0)));

        // A second shard is refused, and tried again after the delay, in case another shard left meanwhile.
        measure(cache, shard(1), GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard(1), GENERATION_1, commit), equalTo(new NotMeasured(Reason.OVER_BUDGET)));

        // The first shard's rebuild after a snapshot is never refused, even when its new baseline is bigger than the budget.
        final var files = new LuceneFile[20];
        for (int i = 0; i < files.length; i++) {
            files[i] = file("_" + i + ".cfs", 1000);
        }
        final var grown = new FakeShard(null, files);
        repository.metadata.put(GENERATION_2, backedUp(files));
        assertThat(ShardBaseline.of(backedUp(files), grown.files).ramBytesUsed(), greaterThan(budget));
        measure(cache, shard(0), GENERATION_2, grown);
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard(0), GENERATION_2, grown), equalTo(new Measured(GENERATION_2, 0)));

        // Once the first shard leaves, the second gets its baseline on its next retry.
        cache.retain(Set.of(REPOSITORY), Set.of(shard(1)));
        nowMillis += ShardBaselineCache.FIRST_RETRY_DELAY.millis();
        measure(cache, shard(1), GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();
        assertThat(measure(cache, shard(1), GENERATION_1, commit), equalTo(new Measured(GENERATION_1, 0)));
    }

    public void testOnceTheBudgetIsFullANewShardIsRefusedWithoutARead() {
        final var commit = new FakeShard(null, file("_0.cfs", 1000));
        repository.metadata.put(GENERATION_1, backedUp(file("_0.cfs", 1000)));
        // Exactly one baseline fills the budget.
        final var cache = cache(ShardBaseline.of(backedUp(file("_0.cfs", 1000)), commit.files).ramBytesUsed());
        measure(cache, shard(0), GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();

        // A second shard couldn't fit whatever its metadata holds, so the repository isn't read for it.
        assertThat(measure(cache, shard(1), GENERATION_1, commit), equalTo(new NotMeasured(Reason.OVER_BUDGET)));
        assertFalse(taskQueue.hasRunnableTasks());
        assertThat(repository.reads, equalTo(List.of(GENERATION_1)));
    }

    public void testAShardThatLeavesIsForgotten() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final var leaving = shard(0);
        final var staying = shard(1);
        measure(cache, leaving, GENERATION_1, new FakeShard(null));
        measure(cache, staying, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();
        final long both = cache.ramBytesUsed();

        cache.retain(Set.of(REPOSITORY), Set.of(staying));

        assertThat(cache.ramBytesUsed(), equalTo(both / 2));
        assertThat(measure(cache, staying, GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
        // If the shard comes back, it starts again.
        assertThat(measure(cache, leaving, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
    }

    public void testALoadForAShardThatLeftChangesNothing() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.metadata.put(GENERATION_1, backedUp());
        // The shard leaves the node while its metadata is being read.
        repository.duringRead = generation -> cache.retain(Set.of(REPOSITORY), Set.of());

        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        assertThat(cache.ramBytesUsed(), equalTo(0L));
        assertThat(measure(cache, shard, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
    }

    public void testARepositoryNoLongerMeasuredIsForgotten() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final var otherRepository = new ProjectRepo(ProjectId.DEFAULT, "other-repo");
        final var shard = shard(0);
        final var commit = new FakeShard(null, file("_0.cfs", 100));
        measure(cache, shard, GENERATION_1, commit);
        taskQueue.runAllRunnableTasks();
        final long one = cache.ramBytesUsed();
        // The other repository has nothing of the shard, so its baseline needs no read.
        cache.measure(otherRepository, shard, INDEX_ID, ShardGenerations.NEW_SHARD_GEN, commit);
        taskQueue.runAllRunnableTasks();
        assertThat(cache.ramBytesUsed() > one, equalTo(true));

        // For example, the policy that named the other repository was deleted.
        cache.retain(Set.of(REPOSITORY), Set.of(shard));

        assertThat(cache.ramBytesUsed(), equalTo(one));
        assertThat(measure(cache, shard, GENERATION_1, commit), equalTo(new Measured(GENERATION_1, 100)));
        assertThat(
            cache.measure(otherRepository, shard, INDEX_ID, ShardGenerations.NEW_SHARD_GEN, commit),
            equalTo(new NotMeasured(Reason.LOADING))
        );
    }

    public void testAClosedShardIsForgottenAtOnce() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final var closed = shard(0);
        final var open = shard(1);
        measure(cache, closed, GENERATION_1, new FakeShard(null));
        measure(cache, open, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();
        final long both = cache.ramBytesUsed();

        cache.removeShard(closed);

        assertThat(cache.ramBytesUsed(), equalTo(both / 2));
        assertThat(measure(cache, open, GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
        // Another copy of the same shard is a different shard copy, so removing one never drops the other.
        cache.removeShard(new ShardCopy(open.shardId(), "another-copy"));
        assertThat(measure(cache, open, GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
    }

    public void testAnotherCopyOfTheShardStartsAgain() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        final var shardId = new ShardId("index", "index-uuid", 0);
        measure(cache, new ShardCopy(shardId, "first-copy"), GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        // The shard relocated away and back between two measures, so this is another copy of it. It might have another history.
        final var secondCopy = new ShardCopy(shardId, "second-copy");
        assertThat(measure(cache, secondCopy, GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.LOADING)));
        assertThat(cache.ramBytesUsed(), equalTo(0L));
        taskQueue.runAllRunnableTasks();

        assertThat(repository.reads, equalTo(List.of(GENERATION_1, GENERATION_1)));
        assertThat(measure(cache, secondCopy, GENERATION_1, new FakeShard(null)), equalTo(new Measured(GENERATION_1, 0)));
    }

    public void testClosingDropsEverythingAndStopsLoads() {
        final var cache = cache(Long.MAX_VALUE);
        repository.metadata.put(GENERATION_1, backedUp());
        measure(cache, shard(0), GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();
        // A load is queued when measuring is turned off.
        measure(cache, shard(1), GENERATION_1, new FakeShard(null));

        cache.close();
        taskQueue.runAllRunnableTasks();

        assertThat(repository.reads, equalTo(List.of(GENERATION_1)));
        assertThat(cache.ramBytesUsed(), equalTo(0L));
        assertThat(measure(cache, shard(0), GENERATION_1, new FakeShard(null)), equalTo(new NotMeasured(Reason.CLOSED)));
    }

    public void testACommitThatCannotBeTakenIsNotMeasured() {
        final var cache = cache(Long.MAX_VALUE);
        final var shard = shard(0);
        repository.metadata.put(GENERATION_1, backedUp());
        measure(cache, shard, GENERATION_1, new FakeShard(null));
        taskQueue.runAllRunnableTasks();

        // For example, the shard is closing.
        final var closing = new FakeShard(null);
        closing.failure = new IOException("simulated");
        assertThat(measure(cache, shard, GENERATION_1, closing), equalTo(new NotMeasured(Reason.READ_FAILED)));
    }

    private ShardBaselineCache cache(long budgetBytes) {
        return new ShardBaselineCache(repository, taskQueue::scheduleNow, () -> nowMillis, budgetBytes);
    }

    private static Result measure(ShardBaselineCache cache, ShardCopy shard, ShardGeneration generation, FakeShard commit) {
        return cache.measure(REPOSITORY, shard, INDEX_ID, generation, commit);
    }

    private static ShardCopy shard(int id) {
        return new ShardCopy(new ShardId("index", "index-uuid", id), "copy-" + id);
    }

    /**
     * A file of the shard. Its writer uuid is derived from its name, so a test can build the same file twice.
     */
    private static LuceneFile file(String name, long length) {
        return new LuceneFile(name, length, new BytesRef(name));
    }

    /**
     * Backup metadata with one snapshot that has these files.
     */
    private static BlobStoreIndexShardSnapshots backedUp(LuceneFile... files) {
        final List<FileInfo> fileInfos = new ArrayList<>();
        for (LuceneFile file : files) {
            final var metadata = new StoreFileMetadata(
                file.name(),
                file.length(),
                Store.digestToString(file.length()),
                Version.LATEST.toString(),
                null,
                file.writerUuid()
            );
            fileInfos.add(new FileInfo("__" + file.name(), metadata, null));
        }
        return BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", fileInfos, null));
    }

    /**
     * A repository with backup metadata by generation. A read builds the baseline from the whole metadata, which gives the same
     * baseline as reading it one entry at a time ({@link ShardBaselineFormatTests}). A read fails if {@link #failure} is set, or if
     * there is no metadata for the generation, as when a newer generation replaced it. {@link #duringRead} runs in the middle of each
     * read.
     */
    private class FakeRepository implements ShardBaselineCache.MetadataReader {
        final Map<ShardGeneration, BlobStoreIndexShardSnapshots> metadata = new HashMap<>();
        final List<ShardGeneration> reads = new ArrayList<>();
        IOException failure;
        Consumer<ShardGeneration> duringRead = generation -> {};

        @Override
        public ShardBaseline read(
            ProjectRepo repository,
            IndexId indexId,
            int shardId,
            ShardGeneration generation,
            List<LuceneFile> currentFiles
        ) throws IOException {
            assertThat(repository, equalTo(REPOSITORY));
            assertThat(indexId, equalTo(INDEX_ID));
            events.add("read " + generation);
            reads.add(generation);
            duringRead.accept(generation);
            if (failure != null) {
                throw failure;
            }
            final var shardSnapshots = metadata.get(generation);
            if (shardSnapshots == null) {
                throw new NoSuchFileException("index-" + generation);
            }
            return ShardBaseline.of(shardSnapshots, currentFiles);
        }
    }

    /**
     * A shard whose latest commit has the given state id and files, or that fails to take it if {@link #failure} is set.
     */
    private class FakeShard implements ShardBaselineCache.LatestCommits {
        final String stateId;
        final List<LuceneFile> files;
        IOException failure;

        FakeShard(String stateId, LuceneFile... files) {
            this.stateId = stateId;
            this.files = List.of(files);
        }

        @Override
        public LatestCommit get() throws IOException {
            events.add("commit");
            if (failure != null) {
                throw failure;
            }
            return new LatestCommit(stateId, files);
        }
    }
}
