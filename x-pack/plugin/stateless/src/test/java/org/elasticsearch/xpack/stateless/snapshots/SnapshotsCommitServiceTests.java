/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.AdvancingTimeProvider;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StaleCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService.RecoveredCommitsReleasable;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.clusterStateWithPrimaryAndSearchShards;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.markDeletedAndLocalUnused;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.markLocalUnused;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.readIndexingShardState;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.waitUntilBCCIsUploaded;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService.RECOVERY_RETAINED_COMMITS_HISTOGRAM;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService.RECOVERY_RETAINED_DURATION_HISTOGRAM;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService.SHARD_RELOCATED_TOTAL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link SnapshotsCommitService}, which holds the commits of the shards that are being snapshotted and gives a snapshot the
 * location of each file of its commit.
 * <p>
 * Terms used in the tests:
 * <ul>
 * <li>A commit blob is a blob in the object store that holds the files of one or more commits of a shard.</li>
 * <li>A commit header is the start of each commit in a commit blob. It lists the files of the commit and, from that, where each file
 * is stored.</li>
 * <li>A generational file is a file that a segment gets after the segment was first written, when a document in the segment is updated
 * or deleted. The shard copies each generational file that is still in use into the first commit of every new commit blob.</li>
 * <li>A commit carries a generational file when the file first appeared in an older commit blob than the commit's own.</li>
 * </ul>
 */
public class SnapshotsCommitServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private int primaryTerm;
    // the reads with the purpose SNAPSHOT_METADATA that the object store of a test fixture has served, see MetadataReadContainer
    private final Queue<MetadataRead> metadataReads = ConcurrentCollections.newQueue();
    private volatile MetadataReadOverride metadataReadOverride;

    @Before
    public void initThreadPool() throws Exception {
        threadPool = new TestThreadPool(SnapshotsCommitServiceTests.class.getName(), Settings.EMPTY);
        primaryTerm = randomIntBetween(1, 100);
    }

    @After
    public void terminateThreadPool() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testLocalSnapshotRetainsCommits() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Generate and upload commits
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef commit : initialCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }

            // Set up cluster state with a running snapshot, then register the latest commit for it
            final var commitRefReleased = new AtomicBoolean(false);
            final var acquiredCommit = initialCommits.getLast();
            final var snapshotIndexCommit = new SnapshotIndexCommit(
                new Engine.IndexCommitRef(acquiredCommit, () -> commitRefReleased.set(true))
            );
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);
            testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);

            // Create a merge commit so the old commits are eligible for deletion
            final var mergedCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());
            // Mark all initial commits locally unused, all but the last of initial commits (the acquired commit) locally deleted.
            markDeletedAndLocalUnused(
                List.of(mergedCommit, acquiredCommit),
                initialCommits.subList(0, initialCommits.size() - 1),
                commitService,
                shardId
            );
            markLocalUnused(List.of(mergedCommit), List.of(acquiredCommit), commitService, shardId);

            // The registered commit should not be released due to acquired by snapshot, no deletion either
            assertFalse(commitRefReleased.get());
            assertThat(deletedCommits, empty());

            if (randomBoolean()) {
                // Simulate shard closed. Commit ref released after shard close does not trigger any more commit deletion
                commitService.closeShard(shardId);
                testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                assertThat(deletedCommits, empty());
            } else {
                // Simulate snapshot completion. The commit is released and deleted
                snapshotIndexCommit.closingBefore(ActionListener.noop()).onResponse(null);
                commitService.markCommitDeleted(shardId, acquiredCommit.getGeneration());
                assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId))));
            }
            // The commit ref is always released
            assertTrue(commitRefReleased.get());
        }
    }

    private enum AfterRecoveryAction {
        COMPLETE_SNAPSHOT,
        RELOCATE_AGAIN,
        DELETE_INDEX
    }

    public void testRecoveryRetainsRefsForRunningSnapshots() throws Exception {
        for (AfterRecoveryAction afterRecoveryAction : AfterRecoveryAction.values()) {
            logger.info("--> testing with [{}]", afterRecoveryAction);
            doTestRecoveryRetainsRefsForRunningSnapshots(afterRecoveryAction);
        }
    }

    public void doTestRecoveryRetainsRefsForRunningSnapshots(AfterRecoveryAction afterRecoveryAction) throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload initial commits (pre-recovery BCC chain)
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            // Set up cluster state with a running snapshot targeting this shard in INIT state
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            // Read state for recovery, then close and re-register the shard
            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            // Recovery should detect the running snapshots and inc refs for all recovered blob references
            commitService.markRecoveredBcc(
                shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs(),
                testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId)
            );

            // After recovery, create new commits and a merge that subsumes them
            final var postRecoveryCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 4));
            for (StatelessCommitRef commit : postRecoveryCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }
            final var mergeCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergeCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergeCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergeCommit.getGeneration());

            // Mark recovered commit and post-recovery commits as deleted and local-unused (the merge subsumes them)
            markDeletedAndLocalUnused(
                List.of(mergeCommit),
                CollectionUtils.appendToCopy(postRecoveryCommits, initialCommits.getLast()),
                commitService,
                shardId
            );

            // Post-recovery commits should be deleted because they have no snapshot refs.
            // But pre-recovery commits should still be retained for the ongoing snapshot.
            assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(postRecoveryCommits, shardId))));

            switch (afterRecoveryAction) {
                case COMPLETE_SNAPSHOT -> {
                    // Complete the snapshot via cluster state update which releases recovery refs
                    final var stateWithoutSnapshot = randomBoolean()
                        ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                        : advanceShardSnapshotState(stateWithSnapshot, snapshot, shardId);
                    ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
                    assertBusy(
                        () -> assertThat(
                            deletedCommits,
                            hasItems(staleCommits(initialCommits, shardId).toArray(StaleCompoundCommit[]::new))
                        )
                    );
                }
                case RELOCATE_AGAIN -> {
                    commitService.closeShard(shardId);
                    testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                    if (randomBoolean()) {
                        commitService.unregister(shardId);
                    }
                    // Index is not deleted, commits retained on recovery are _not_ deleted, the new primary shall handle them
                    assertThat(deletedCommits, equalTo(staleCommits(postRecoveryCommits, shardId)));
                }
                case DELETE_INDEX -> {
                    commitService.markIndexDeleting(List.of(shardId));
                    commitService.closeShard(shardId);
                    testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                    commitService.delete(shardId);
                    if (randomBoolean()) {
                        commitService.unregister(shardId);
                    }
                    // Index is being deleted: snapshot commit tracking is retained so BCC blobs stay alive until the
                    // snapshot completes or aborts. The retained stale commits must not be deleted while the snapshot
                    // is still active.
                    assertTrue(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
                    for (var staleCommit : staleCommits(initialCommits, shardId)) {
                        assertFalse(
                            "initial commit " + staleCommit + " should not be deleted while snapshot is active",
                            deletedCommits.contains(staleCommit)
                        );
                    }

                    // Complete the snapshot via cluster state update. SnapshotsCommitService.clusterChanged then
                    // closes the retained releasable, which finally releases the commits for deletion.
                    final var stateWithoutSnapshot = removeSnapshotFromClusterState(stateWithSnapshot, snapshot);
                    ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
                    assertBusy(
                        () -> assertThat(
                            deletedCommits,
                            hasItems(staleCommits(initialCommits, shardId).toArray(StaleCompoundCommit[]::new))
                        )
                    );
                    assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
                }
            }
        }
    }

    public void testRemoteSnapshotRefsReplacedForTheSameSnapshot() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload initial commits (pre-recovery BCC chain)
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            // Set up cluster state with a running snapshot, then perform recovery which retains all commits for the snapshot
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            commitService.markRecoveredBcc(
                shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs(),
                testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId)
            );

            // After recovery, create a merge commit
            final var mergeCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergeCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergeCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergeCommit.getGeneration());

            // Mark recovered BCC as deleted and local-unused, no deletion yet due to snapshot
            markDeletedAndLocalUnused(List.of(mergeCommit), List.of(initialCommits.getLast()), commitService, shardId);
            assertThat(deletedCommits, empty());

            // Register the same snapshot for the merge commit via registerReleaseForSnapshot.
            // This replaces the broad recovery ref with one specific for the merge commit,
            // allowing the pre-recovery commits to be deleted.
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(mergeCommit, () -> {}));
            testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);

            assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId))));
        }
    }

    public void testRaceBetweenSnapshotRegisterAndShardClosed() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);
            final var commitRefReleased = new AtomicBoolean(false);
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(commit, () -> commitRefReleased.set(true)));

            // Race between concurrent snapshot register and shard closing
            final var barrier = new CyclicBarrier(2);
            final var registerThread = new Thread(() -> {
                safeAwait(barrier);
                testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
            });
            final var releaseThread = new Thread(() -> {
                safeAwait(barrier);
                commitService.closeShard(shardId);
                testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            });

            registerThread.start();
            releaseThread.start();
            registerThread.join(30_000);
            releaseThread.join(30_000);
            assertTrue("commit ref should be released after concurrent register and shard close", commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testRaceBetweenSnapshotRegisterAndSnapshotCompletion() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var stateWithoutSnapshot = randomBoolean()
                ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                : failShardSnapshotState(stateWithSnapshot, snapshot, shardId);

            final var commitRefReleased = new AtomicBoolean(false);
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(commit, () -> commitRefReleased.set(true)));

            // Race between a lagging remote node registering and the snapshot completing/failing
            final var barrier = new CyclicBarrier(2);
            final var registerThread = new Thread(() -> {
                safeAwait(barrier);
                try {
                    testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
                } catch (IndexShardSnapshotFailedException e) {
                    // registerReleaseForSnapshot may throw IndexShardSnapshotFailedException if the snapshot
                    // completes before or during registration — this is expected in the race.
                }
            });
            final var completionThread = new Thread(() -> {
                safeAwait(barrier);
                ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
            });

            registerThread.start();
            completionThread.start();
            registerThread.join(30_000);
            completionThread.join(30_000);
            assertTrue("commit ref should be released after concurrent register and snapshot completion", commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testRaceBetweenRetainingCommitsOnRecoveryAndSnapshotCompletion() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef commit : initialCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            // Close, unregister, and re-register the shard to simulate a recovery
            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            // Race between concurrent recovery and snapshot completion
            final var releasableReleased = new AtomicBoolean(false);
            final var extraReferenceConsumers = new ArrayList<Consumer<RecoveredCommitsReleasable>>();
            testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId).forEachRemaining(extraReferenceConsumers::add);
            assertThat(extraReferenceConsumers.size(), equalTo(1));
            final Iterator<Consumer<RecoveredCommitsReleasable>> consumerIterator = Iterators.map(
                extraReferenceConsumers.iterator(),
                c -> r -> {
                    c.accept(new RecoveredCommitsReleasable(Releasables.releaseOnce(() -> {
                        releasableReleased.set(true);
                        Releasables.close(r);
                    }), r.size()));
                }
            );
            final var stateWithoutSnapshot = removeSnapshotFromClusterState(stateWithSnapshot, snapshot);

            final var barrier = new CyclicBarrier(2);
            final var recoveryThread = new Thread(() -> {
                safeAwait(barrier);
                commitService.markRecoveredBcc(
                    shardId,
                    indexingShardState.latestCommit(),
                    indexingShardState.otherBlobs(),
                    consumerIterator
                );
            });
            final var clusterChangedThread = new Thread(() -> {
                safeAwait(barrier);
                ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
            });

            recoveryThread.start();
            clusterChangedThread.start();
            recoveryThread.join(30_000);
            clusterChangedThread.join(30_000);
            assertTrue("releasable should be released after concurrent recovery and snapshot completion", releasableReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testAcquireAndMaybeRegisterSuccess() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload a commit with a file
            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            // Set up cluster state with a running snapshot
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var commitRefReleased = new AtomicBoolean(false);
            mockIndexShardAndCommit(testHarness, commit, commitRefReleased);

            final boolean supportsRelocation = randomBoolean();
            final var result = testHarness.snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(
                shardId,
                snapshot,
                supportsRelocation,
                null
            );

            assertNotNull(result);
            assertThat(result.blobLocations().keySet(), equalTo(Set.copyOf(commit.getFileNames().stream().toList())));
            result.blobLocations().values().forEach(loc -> assertNotNull(loc));
            assertFalse(commitRefReleased.get());

            if (supportsRelocation) {
                // Commit is registered — SnapshotsCommitService tracks it
                assertTrue(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
            } else {
                // Commit is NOT registered — caller is responsible for the lifecycle
                assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
            }
        }
    }

    /**
     * Each commit blob starts with copies of the generational files that were in use then. So a commit that carries a generational file
     * has a copy of the file in its own commit blob, and the header of the commit lists that copy. The snapshot gets the location of that
     * copy, both when the commit is the first one in its commit blob and when the commit comes later. A generational file that a commit
     * wrote into the same commit blob keeps the location where it first appeared, which is already in that commit blob.
     */
    public void testCarriedGenerationalFilesPointAtTheCopyInTheCommitsOwnCommitBlob() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            // The first commit blob has generational files, which the later commit blobs carry. The second commit blob has commits that
            // come after a commit that wrote generational files into the same commit blob.
            final var uploadedCommits = uploadCommitBlobs(testHarness, between(2, 3), 3, between(1, 3));

            boolean carriedByFirstCommit = false;
            boolean carriedByLaterCommit = false;
            boolean keptByLaterCommit = false;
            for (UploadedCommit uploaded : uploadedCommits) {
                final var snapshotLocations = acquireCommitForSnapshot(testHarness, uploaded.commit(), new AtomicBoolean()).blobLocations();

                // the snapshot gets the locations that the header of the commit itself lists
                assertThat(snapshotLocations, equalTo(readCommitHeader(testHarness, uploaded).commitFiles()));

                for (var entry : snapshotLocations.entrySet()) {
                    if (StatelessCompoundCommit.isGenerationalFile(entry.getKey()) == false) {
                        continue;
                    }
                    // the location where the commit service saw the file first, and where a snapshot took the file from before
                    final BlobLocation firstLocation = testHarness.commitService.getBlobLocation(testHarness.shardId, entry.getKey());
                    if (firstLocation.equals(entry.getValue())) {
                        keptByLaterCommit |= uploaded.isFirstInBlob() == false
                            && firstLocation.compoundFileGeneration() == uploaded.blobGeneration();
                    } else {
                        assertThat(firstLocation.compoundFileGeneration(), lessThan(uploaded.blobGeneration()));
                        assertThat(entry.getValue().compoundFileGeneration(), equalTo(uploaded.blobGeneration()));
                        carriedByFirstCommit |= uploaded.isFirstInBlob();
                        carriedByLaterCommit |= uploaded.isFirstInBlob() == false;
                    }
                }
            }
            assertTrue("no first commit of a commit blob carries a generational file", carriedByFirstCommit);
            assertTrue("no later commit of a commit blob carries a generational file", carriedByLaterCommit);
            assertTrue("no later commit keeps a generational file that was written into its commit blob", keptByLaterCommit);
        }
    }

    /**
     * A commit that carries generational files needs exactly one GET: the start of its commit blob, up to its {@code segments_N} file.
     * The start of the commit blob is the header of its first commit, which lists a copy of each carried file. Assertions are on in a
     * test. They would trip if the read went through the iterator over all the commits of the commit blob, with the position of
     * {@code segments_N} as its end. That fails when the snapshotted commit is the first commit of the blob, because that commit extends
     * past the end. The assertions would also trip if the read checked the padding between two commits with a second GET. A commit that
     * carries no generational file needs no GET.
     */
    public void testCarriedGenerationalFilesNeedOneReadOfTheStartOfTheCommitBlob() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var uploadedCommits = uploadCommitBlobs(testHarness, between(2, 3), 3, between(1, 3));

            boolean readForFirstCommit = false;
            boolean readForLaterCommit = false;
            boolean noReadForCommit = false;
            for (UploadedCommit uploaded : uploadedCommits) {
                boolean carriesGenerationalFile = false;
                for (String file : uploaded.commit().getFileNames()) {
                    final var firstLocation = testHarness.commitService.getBlobLocation(testHarness.shardId, file);
                    carriesGenerationalFile |= StatelessCompoundCommit.isGenerationalFile(file)
                        && firstLocation.compoundFileGeneration() != uploaded.blobGeneration();
                }

                metadataReads.clear();
                acquireCommitForSnapshot(testHarness, uploaded.commit(), new AtomicBoolean());

                if (carriesGenerationalFile) {
                    final var segmentsFile = testHarness.commitService.getBlobLocation(
                        testHarness.shardId,
                        uploaded.commit().getSegmentsFileName()
                    );
                    final var blobName = BatchedCompoundCommit.blobNameFromGeneration(uploaded.blobGeneration());
                    assertThat(List.copyOf(metadataReads), equalTo(List.of(new MetadataRead(blobName, 0L, segmentsFile.offset()))));
                    readForFirstCommit |= uploaded.isFirstInBlob();
                    readForLaterCommit |= uploaded.isFirstInBlob() == false;
                } else {
                    assertThat(metadataReads, empty());
                    noReadForCommit = true;
                }
            }
            assertTrue("no first commit of a commit blob needed a read", readForFirstCommit);
            assertTrue("no later commit of a commit blob needed a read", readForLaterCommit);
            assertTrue("every commit needed a read", noReadForCommit);
        }
    }

    /**
     * The shard snapshot fails, and the commit is released, when the commit blob cannot give the location of a carried generational file:
     * the read fails, or the first header has no copy of the file in the commit's own commit blob. The snapshot does not fall back to the
     * first location of the file.
     */
    public void testHeaderThatCannotGiveTheLocationOfACarriedFileFailsTheShardSnapshot() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var uploadedCommits = uploadCommitBlobs(testHarness, 2, 2);
            final boolean readFails = randomBoolean();
            final String firstBlobName = BatchedCompoundCommit.blobNameFromGeneration(uploadedCommits.getFirst().blobGeneration());
            metadataReadOverride = (container, read) -> {
                if (readFails) {
                    throw new IOException("simulated failure of the read");
                }
                // The start of the first commit blob has the header of a commit with no generational file.
                final long firstBlobLength = container.listBlobs(OperationPurpose.INDICES).get(firstBlobName).length();
                return container.readBlob(OperationPurpose.SNAPSHOT_METADATA, firstBlobName, 0L, firstBlobLength);
            };

            final var commitRefReleased = new AtomicBoolean();
            final var failure = expectThrows(
                IndexShardSnapshotFailedException.class,
                () -> acquireCommitForSnapshot(testHarness, uploadedCommits.getLast().commit(), commitRefReleased)
            );
            if (readFails) {
                assertThat(
                    failure.getMessage(),
                    containsString("failed to read the first commit header of commit blob [stateless_commit_")
                );
                assertThat(failure.getCause(), instanceOf(IOException.class));
            } else {
                assertThat(failure.getMessage(), containsString("has no copy of generational file ["));
            }
            assertTrue(commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(testHarness.shardId));
        }
    }

    public void testLatchingBooleanSupplier() {
        final var relocated = new AtomicBoolean(false);
        final var invocations = new AtomicInteger(0);
        final BooleanSupplier source = () -> {
            invocations.incrementAndGet();
            return relocated.get();
        };
        // Constructor consults the delegate once: since source is false, it is retained.
        final var supplier = new SnapshotsCommitService.LatchingBooleanSupplier(source);
        assertThat(invocations.get(), equalTo(1));
        assertSame(source, supplier.delegate());

        // Before the latch, each call goes to the delegate.
        assertFalse(supplier.getAsBoolean());
        assertThat(invocations.get(), equalTo(2));

        // Flipping to true triggers the latch: the delegate is consulted one last time and then released.
        relocated.set(true);
        assertTrue(supplier.getAsBoolean());
        assertThat(invocations.get(), equalTo(3));
        assertNull(supplier.delegate());

        // Subsequent calls stay true without touching the delegate.
        for (int i = 0; i < between(3, 10); i++) {
            assertTrue(supplier.getAsBoolean());
        }
        assertThat(invocations.get(), equalTo(3));
    }

    public void testLatchingBooleanSupplierAlreadyTrueAtConstruction() {
        final var invocations = new AtomicInteger(0);
        final BooleanSupplier source = () -> {
            invocations.incrementAndGet();
            return true;
        };
        // Constructor consults the delegate once, sees true, and releases it immediately.
        final var supplier = new SnapshotsCommitService.LatchingBooleanSupplier(source);
        assertThat(invocations.get(), equalTo(1));
        assertNull(supplier.delegate());

        // Subsequent calls stay true without touching the delegate.
        for (int i = 0; i < between(3, 10); i++) {
            assertTrue(supplier.getAsBoolean());
        }
        assertThat(invocations.get(), equalTo(1));
    }

    public void testCommitIsReleasedWhenAcquireAndMaybeRegisterThrows() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload a commit
            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            // Set up cluster state with a running snapshot
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var commitRefReleased = new AtomicBoolean(false);
            final var shardAndCommit = mockIndexShardAndCommit(testHarness, commit, commitRefReleased);

            // Randomly pick which operation fails after the commit is acquired
            final Exception expectedException;
            final int failureVariant = between(0, 2);
            switch (failureVariant) {
                case 0 -> {
                    // getShardStateId fails via indexCommit.getUserData()
                    expectedException = new IOException("simulated getUserData failure");
                    when(shardAndCommit.indexCommit().getUserData()).thenThrow(expectedException);
                }
                case 1 -> {
                    // getFileNames fails
                    expectedException = new IOException("simulated getFileNames failure");
                    when(shardAndCommit.indexCommit().getFileNames()).thenThrow(expectedException);
                }
                default -> {
                    // Simulate concurrent shard delete
                    commitService.closeShard(shardId);
                    commitService.unregister(shardId);
                    expectedException = new AlreadyClosedException("shard closed");
                }
            }

            expectThrows(
                expectedException.getClass(),
                () -> testHarness.snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(shardId, snapshot, false, null)
            );
            assertTrue(commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testRaceBetweenAbortAndAcquire() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var commitRefReleased = new AtomicBoolean(false);
            mockIndexShardAndCommit(testHarness, commit, commitRefReleased);
            // TODO: randomize in a future PR, see also ES-14099
            final boolean supportsRelocation = false;
            final var snapshotStatus = IndexShardSnapshotStatus.newInitializing(null, System.currentTimeMillis());

            final var raceBarrier = new CyclicBarrier(3);
            final var abortThread = new Thread(() -> {
                safeAwait(raceBarrier);
                snapshotStatus.abortIfNotCompleted("concurrent abort for test", listener -> listener.onResponse(null));
            }, "TEST-abort-thread");
            final var acquireResult = new AtomicReference<SnapshotsCommitService.SnapshotCommitInfo>();
            final var acquireThread = new Thread(() -> {
                safeAwait(raceBarrier);
                try {
                    acquireResult.set(
                        testHarness.snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(
                            shardId,
                            snapshot,
                            supportsRelocation,
                            snapshotStatus
                        )
                    );
                } catch (Exception ignored) {
                    // Expected when the abort is detected by ensureNotAborted
                }
            }, "TEST-acquire-thread");

            abortThread.start();
            acquireThread.start();
            safeAwait(raceBarrier);
            safeJoin(abortThread);
            safeJoin(acquireThread);

            // The SnapshotIndexCommit must always be released regardless of abort timing
            assertTrue(commitRefReleased.get());

            if (acquireResult.get() != null) {
                // If the acquire method returned a result, the concurrent abort happens after it. The service may have created
                // a tracking which will be removed eventually when the snapshot is completed in the cluster state.
                if (supportsRelocation) {
                    assertTrue(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
                    final var stateWithoutSnapshot = randomBoolean()
                        ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                        : failShardSnapshotState(stateWithSnapshot, snapshot, shardId);
                    ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
                }
                assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
            } else {
                assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
            }
        }
    }

    public void testSnapshotMetricsRecordedDuringRelocationAndRecovery() throws Exception {
        final var meterRegistry = new RecordingMeterRegistry();
        final var timeProvider = new AdvancingTimeProvider();
        try (var fixture = createTestFixture(meterRegistry, timeProvider)) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;
            final var snapshotsCommitService = testHarness.snapshotsCommitService;

            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef commit : initialCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }

            // Set up cluster state with a running snapshot, then register the latest commit for it
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);
            snapshotsCommitService.registerReleaseForSnapshot(
                shardId,
                snapshot,
                new SnapshotIndexCommit(new Engine.IndexCommitRef(initialCommits.getLast(), () -> {}))
            );

            // Simulate relocation
            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            final var relocationCounter = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, SHARD_RELOCATED_TOTAL);
            assertThat(relocationCounter, hasSize(1));
            assertThat(relocationCounter.getFirst().getLong(), equalTo(1L));

            // Simulate recovery
            commitService.markRecoveredBcc(
                shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs(),
                snapshotsCommitService.getExtraReferenceConsumers(shardId)
            );

            final var commitsHistogram = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_HISTOGRAM, RECOVERY_RETAINED_COMMITS_HISTOGRAM);
            assertThat(commitsHistogram, hasSize(1));
            assertThat(commitsHistogram.getFirst().getLong(), equalTo((long) initialCommits.size()));

            // Simulate snapshot completion
            timeProvider.advanceByMillis(randomLongBetween(1, 10_000));
            ClusterServiceUtils.setState(
                testHarness.clusterService,
                randomBoolean()
                    ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                    : failShardSnapshotState(stateWithSnapshot, snapshot, shardId)
            );

            final var durationHistogram = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, RECOVERY_RETAINED_DURATION_HISTOGRAM);
            assertThat(durationHistogram, hasSize(1));
            assertThat(durationHistogram.getFirst().getDouble(), greaterThan(0.0));

            // The snapshot completion does not trigger the relocation counter
            assertThat(meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, SHARD_RELOCATED_TOTAL), hasSize(1));
        }
    }

    /**
     * A commit that a test has uploaded, and the generation of its commit blob, which is the generation of the first commit in the blob.
     */
    private record UploadedCommit(StatelessCommitRef commit, long blobGeneration) {
        boolean isFirstInBlob() {
            return commit.getGeneration() == blobGeneration;
        }
    }

    /**
     * Makes commits with generational files that stay the same in the later commits, see
     * {@link FakeStatelessNode#generateCommitsWithUnchangedGenerationalFiles}, and uploads them in commit blobs of the given sizes. The
     * commit blob of each group ends with the last commit of the group.
     */
    private static List<UploadedCommit> uploadCommitBlobs(FakeStatelessNode testHarness, int... commitsPerBlob) throws IOException {
        final var commits = testHarness.generateCommitsWithUnchangedGenerationalFiles(IntStream.of(commitsPerBlob).sum());
        final var uploadedCommits = new ArrayList<UploadedCommit>();
        int next = 0;
        for (int size : commitsPerBlob) {
            final var commitsOfBlob = commits.subList(next, next + size);
            next += size;
            for (StatelessCommitRef commit : commitsOfBlob) {
                testHarness.commitService.onCommitCreation(commit);
            }
            final long lastGeneration = commitsOfBlob.getLast().getGeneration();
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, lastGeneration);
            waitUntilBCCIsUploaded(testHarness.commitService, testHarness.shardId, lastGeneration);
            for (StatelessCommitRef commit : commitsOfBlob) {
                uploadedCommits.add(new UploadedCommit(commit, commitsOfBlob.getFirst().getGeneration()));
            }
        }
        return uploadedCommits;
    }

    /**
     * Acquires the commit for a snapshot that is in progress, as the primary does. The caller has to release the commit.
     */
    private SnapshotsCommitService.SnapshotCommitInfo acquireCommitForSnapshot(
        FakeStatelessNode testHarness,
        StatelessCommitRef commit,
        AtomicBoolean commitRefReleased
    ) throws IOException {
        final var snapshot = randomSnapshot();
        ClusterServiceUtils.setState(testHarness.clusterService, createClusterStateWithSnapshotsInProgress(testHarness, snapshot));
        mockIndexShardAndCommit(testHarness, commit, commitRefReleased);
        return testHarness.snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(testHarness.shardId, snapshot, false, null);
    }

    /**
     * Reads the header that the commit blob has for the commit, from the object store.
     */
    private static StatelessCompoundCommit readCommitHeader(FakeStatelessNode testHarness, UploadedCommit uploaded) throws IOException {
        final var container = testHarness.getShardContainer();
        final String blobName = BatchedCompoundCommit.blobNameFromGeneration(uploaded.blobGeneration());
        final var commitBlob = BatchedCompoundCommit.readFromStore(
            blobName,
            container.listBlobs(OperationPurpose.INDICES).get(blobName).length(),
            (name, offset, length) -> new InputStreamStreamInput(container.readBlob(OperationPurpose.INDICES, name, offset, length)),
            true
        );
        return commitBlob.compoundCommits()
            .stream()
            .filter(commit -> commit.generation() == uploaded.commit().getGeneration())
            .findFirst()
            .orElseThrow();
    }

    private record MetadataRead(String blobName, long position, long length) {}

    /**
     * Serves a read with the purpose SNAPSHOT_METADATA instead of the object store.
     */
    @FunctionalInterface
    private interface MetadataReadOverride {
        InputStream read(BlobContainer container, MetadataRead read) throws IOException;
    }

    /**
     * Records the reads with the purpose SNAPSHOT_METADATA in {@link #metadataReads}. These are the reads that a snapshot makes of the
     * object store to learn what the commits hold. If a test sets {@link #metadataReadOverride}, that serves the read instead.
     */
    private final class MetadataReadContainer extends FilterBlobContainer {
        private final BlobContainer container;

        MetadataReadContainer(BlobContainer container) {
            super(container);
            this.container = container;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new MetadataReadContainer(child);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                metadataReads.add(new MetadataRead(blobName, 0L, -1L));
            }
            return super.readBlob(purpose, blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                final var read = new MetadataRead(blobName, position, length);
                metadataReads.add(read);
                final var override = metadataReadOverride;
                if (override != null) {
                    return override.read(container, read);
                }
            }
            return super.readBlob(purpose, blobName, position, length);
        }
    }

    private record ShardAndCommit(IndexShard indexShard, IndexCommit indexCommit) {}

    private static ShardAndCommit mockIndexShardAndCommit(
        FakeStatelessNode testHarness,
        StatelessCommitRef commit,
        AtomicBoolean commitRefReleased
    ) throws IOException {
        final var shardId = testHarness.shardId;
        final var indexShard = mock(IndexShard.class);
        final var indexService = mock(IndexService.class);
        when(testHarness.indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(
            TestShardRouting.newShardRouting(shardId, randomIdentifier(), true, ShardRoutingState.STARTED)
        );
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        when(indexShard.indexSettings()).thenReturn(testHarness.indexSettings);

        // Mock the index commit, instead of commit.getIndexCommit() so that we can simulate failures
        final var mockIndexCommit = mock(IndexCommit.class);
        when(indexShard.acquireIndexCommitForSnapshot()).thenReturn(
            new Engine.IndexCommitRef(mockIndexCommit, () -> commitRefReleased.set(true))
        );
        // Set up defaults, individual tests can override
        when(mockIndexCommit.getFileNames()).thenReturn(commit.getFileNames().stream().toList());
        when(mockIndexCommit.getSegmentsFileName()).thenReturn(commit.getSegmentsFileName());
        when(mockIndexCommit.getUserData()).thenReturn(Map.of());
        when(indexShard.getLastSyncedGlobalCheckpoint()).thenReturn(randomLongBetween(1, 100));

        return new ShardAndCommit(indexShard, mockIndexCommit);
    }

    private record TestFixture(FakeStatelessNode node, Set<StaleCompoundCommit> deletedCommits) implements Closeable {
        @Override
        public void close() throws IOException {
            node.close();
        }
    }

    private TestFixture createTestFixture() throws IOException {
        return createTestFixture(null, null);
    }

    private TestFixture createTestFixture(RecordingMeterRegistry meterRegistry, TimeProvider timeProvider) throws IOException {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        final var commitCleaner = new StatelessCommitCleaner(null, null, mock(ObjectStoreService.class)) {
            @Override
            protected void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        final var stateRef = new AtomicReference<ClusterState>();
        final var node = new FakeStatelessNode(
            this::newEnvironment,
            this::newNodeEnvironment,
            xContentRegistry(),
            primaryTerm,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            meterRegistry
        ) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId));
            }

            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new MetadataReadContainer(innerContainer);
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected SnapshotsCommitService createSnapshotsCommitService() {
                if (timeProvider == null) {
                    return super.createSnapshotsCommitService();
                }
                return new SnapshotsCommitService(
                    clusterService,
                    indicesService,
                    commitService,
                    objectStoreService,
                    timeProvider,
                    meterRegistry != null ? meterRegistry : telemetryProvider.getMeterRegistry()
                );
            }
        };
        stateRef.set(clusterStateWithPrimaryAndSearchShards(node.shardId, 0));
        return new TestFixture(node, deletedCommits);
    }

    private ClusterState createClusterStateWithSnapshotsInProgress(FakeStatelessNode testHarness, Snapshot... snapshots) {
        var snapshotsInProgress = SnapshotsInProgress.EMPTY;
        for (final var snapshot : snapshots) {
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                SnapshotsInProgress.startedEntry(
                    snapshot,
                    randomBoolean(),
                    randomBoolean(),
                    Map.of(testHarness.shardId.getIndexName(), new IndexId(testHarness.shardId.getIndexName(), randomUUID())),
                    List.of(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    Map.of(
                        testHarness.shardId,
                        new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), SnapshotsInProgress.ShardState.INIT, null)
                    ),
                    Map.of(),
                    IndexVersion.current(),
                    List.of()
                )
            );
        }
        return ClusterState.builder(testHarness.clusterService.state()).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();
    }

    private static ClusterState removeSnapshotFromClusterState(ClusterState clusterState, Snapshot snapshot) {
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterState);
        return ClusterState.builder(clusterState)
            .putCustom(
                SnapshotsInProgress.TYPE,
                snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                    snapshot.getProjectId(),
                    snapshot.getRepository(),
                    snapshotsInProgress.forRepo(snapshot.getProjectId(), snapshot.getRepository())
                        .stream()
                        .filter(entry -> snapshot.equals(entry.snapshot()) == false)
                        .toList()
                )
            )
            .build();
    }

    private ClusterState failShardSnapshotState(ClusterState clusterState, Snapshot snapshot, ShardId shardId) {
        return updateShardSnapshotStatus(
            clusterState,
            snapshot,
            shardId,
            new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), SnapshotsInProgress.ShardState.FAILED, null, "failed")
        );
    }

    private ClusterState advanceShardSnapshotState(ClusterState clusterState, Snapshot snapshot, ShardId shardId) {
        return updateShardSnapshotStatus(
            clusterState,
            snapshot,
            shardId,
            SnapshotsInProgress.ShardSnapshotStatus.success(
                randomIdentifier(),
                new ShardSnapshotResult(new ShardGeneration(randomIdentifier()), ByteSizeValue.ZERO, 0)
            )
        );
    }

    private ClusterState updateShardSnapshotStatus(
        ClusterState clusterState,
        Snapshot snapshot,
        ShardId shardId,
        SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus
    ) {
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterState);
        final var entry = snapshotsInProgress.snapshot(snapshot);
        assert entry != null;
        final var updatedEntry = SnapshotsInProgress.startedEntry(
            snapshot,
            entry.includeGlobalState(),
            entry.partial(),
            entry.indices(),
            entry.dataStreams(),
            entry.startTime(),
            entry.repositoryStateId(),
            Map.of(shardId, shardSnapshotStatus),
            entry.userMetadata(),
            entry.version(),
            entry.featureStates()
        );
        return ClusterState.builder(clusterState)
            .putCustom(
                SnapshotsInProgress.TYPE,
                snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                    snapshot.getProjectId(),
                    snapshot.getRepository(),
                    snapshotsInProgress.forRepo(snapshot.getProjectId(), snapshot.getRepository())
                        .stream()
                        .map(e -> e.snapshot().equals(snapshot) ? updatedEntry : e)
                        .toList()
                )
            )
            .build();
    }

    private static Snapshot randomSnapshot() {
        return new Snapshot(ProjectId.DEFAULT, randomRepoName(), new SnapshotId(randomSnapshotName(), randomUUID()));
    }

    private Set<StaleCompoundCommit> staleCommits(List<StatelessCommitRef> commits, ShardId shardId) {
        return commits.stream().map(commit -> staleCommit(shardId, commit)).collect(Collectors.toSet());
    }

    private StaleCompoundCommit staleCommit(ShardId shardId, StatelessCommitRef commit) {
        return new StaleCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()), primaryTerm);
    }
}
