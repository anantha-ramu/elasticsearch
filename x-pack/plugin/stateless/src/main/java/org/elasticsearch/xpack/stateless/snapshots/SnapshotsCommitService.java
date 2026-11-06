/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService.RecoveredCommitsReleasable;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.SnapshotShardContextHelper.NOOP_RELEASABLE;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.acquireSnapshotIndexCommit;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.closeSnapshotIndexCommit;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.maybeEnsureNotAborted;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.withSnapshotIndexCommitRef;

/**
 * This class is responsible for tracking and releasing commits needed for ongoing shard snapshots.
 * If a commit is tracked by this class, it is guaranteed to be available in the object store.
 * When a commit is released by this class, the commit becomes a candidate for deletion.
 * Whether it will actually be deleted and when it happens also depend on references from other
 * usages and the shard state, see also {@link StatelessCommitService}.
 */
public class SnapshotsCommitService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotsCommitService.class);

    static final String SHARD_RELOCATED_TOTAL = "es.repositories.snapshots.shards.relocated.total";
    static final String RECOVERY_RETAINED_COMMITS_HISTOGRAM = "es.repositories.snapshots.shards.recovery_retained_commits.histogram";
    static final String RECOVERY_RETAINED_DURATION_HISTOGRAM = "es.repositories.snapshots.shards.recovery_retained_duration.histogram";

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final ObjectStoreService objectStoreService;
    private final TimeProvider timeProvider;
    private final LongCounter shardRelocationDuringSnapshotCounter;
    private final LongHistogram recoveryRetainedCommitsHistogram;
    private final DoubleHistogram recoveryRetainedDurationHistogram;

    // Per-shard map of snapshot to Releasable that prevents commit deletion. Each Releasable, when closed, releases
    // either a SnapshotIndexCommit or BCC blob references.
    //
    // Shard entries are added via active registration with `registerReleaseForSnapshot` or during recovery
    // (`getExtraReferenceConsumers`, retaining the entire BCC chain). They are removed on (1) shard closed
    // or (2) snapshot completion if no more running snapshot for the shard
    //
    // For each shard entry, its members are released and removed (a) on snapshot completion
    // or (b) on snapshot registration, a new member replaces any old entry of the same snapshot
    // or (c) on shard closed.
    private final Map<ShardId, Map<Snapshot, Releasable>> snapshotsCommitReleasables = new ConcurrentHashMap<>();

    public SnapshotsCommitService(
        ClusterService clusterService,
        IndicesService indicesService,
        StatelessCommitService commitService,
        ObjectStoreService objectStoreService,
        TimeProvider timeProvider,
        MeterRegistry meterRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.commitService = commitService;
        this.objectStoreService = objectStoreService;
        this.timeProvider = timeProvider;
        this.shardRelocationDuringSnapshotCounter = meterRegistry.registerLongCounter(
            SHARD_RELOCATED_TOTAL,
            "shard relocated while being snapshotted",
            "unit"
        );
        this.recoveryRetainedCommitsHistogram = meterRegistry.registerLongHistogram(
            RECOVERY_RETAINED_COMMITS_HISTOGRAM,
            "commits retained during recovery for ongoing shard snapshots",
            "unit"
        );
        this.recoveryRetainedDurationHistogram = meterRegistry.registerDoubleHistogram(
            RECOVERY_RETAINED_DURATION_HISTOGRAM,
            "duration till recovery retaining commits are released",
            "s"
        );
    }

    public record SnapshotCommitInfo(
        SnapshotIndexCommit snapshotIndexCommit,
        Map<String, BlobLocation> blobLocations,
        @Nullable String shardStateId,
        BooleanSupplier isShardRelocated
    ) {}

    /**
     * Acquire an index commit for the shard snapshot. If relocation during snapshots is supported, i.e.
     * {@code supportsRelocationDuringSnapshot} = true, also register a releasable for the snapshot so that
     * this class manages the commit's lifecycle. Otherwise, the caller is responsible for releasing the commit.
     */
    public SnapshotCommitInfo acquireAndMaybeRegisterCommitForSnapshot(
        ShardId shardId,
        Snapshot snapshot,
        boolean supportsRelocationDuringSnapshot,
        @Nullable IndexShardSnapshotStatus snapshotStatus // null if the snapshot is running on a remote node
    ) throws IOException {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        assert indexShard.routingEntry().primary() : "get commit info should only be executed on a primary shard";
        final var snapshotIndexCommitAndShardStateId = acquireSnapshotIndexCommit(
            clusterService,
            indexShard,
            snapshot,
            supportsRelocationDuringSnapshot,
            snapshotStatus
        );
        final var snapshotIndexCommit = snapshotIndexCommitAndShardStateId.snapshotIndexCommit();
        final var shardStateId = snapshotIndexCommitAndShardStateId.shardStateId();
        // Local snapshot (snapshotStatus != null) may be concurrently aborted so that we inc-ref before using the index commit.
        try (
            var releasable = withSnapshotIndexCommitRef(
                shardId,
                snapshot.getSnapshotId(),
                snapshotIndexCommit,
                snapshotStatus,
                indexShard::isRelocatedPrimary
            )
        ) {
            assert releasable != NOOP_RELEASABLE || indexShard.isRelocatedPrimary()
                : "unexpected noop releasable for non-relocated shard " + shardId;
            final var indexCommit = snapshotIndexCommit.indexCommit();
            maybeEnsureNotAborted(snapshotStatus);
            final Map<String, BlobLocation> firstLocations = indexCommit.getFileNames()
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Function.identity(),
                        fileName -> Objects.requireNonNull(commitService.getBlobLocation(shardId, fileName))
                    )
                );
            final Map<String, BlobLocation> blobLocations = withGenerationalFilesInOwnCommitBlob(shardId, indexCommit, firstLocations);
            if (supportsRelocationDuringSnapshot) {
                registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
            }
            return new SnapshotCommitInfo(
                snapshotIndexCommit,
                blobLocations,
                shardStateId,
                new LatchingBooleanSupplier(indexShard::isRelocatedPrimary)
            );
        } catch (Exception e) {
            closeSnapshotIndexCommit(snapshotIndexCommit, shardId, snapshot);
            throw e;
        }
    }

    /**
     * Re-points each generational file of the snapshotted commit that was first written in an older commit blob at its copy in the commit
     * blob that holds the commit.
     * <p>
     * A commit blob is a blob in the object store that holds the files of one or more commits. Each commit starts with a header that
     * lists the files of the commit. From the header, a reader can work out where each file is stored: the blob, the offset and the
     * length. A generational file is a file that a segment gets after the segment was first written, when a document in the segment is
     * updated or deleted, such as {@code _0_1.liv}. The shard copies each generational file that is still in use into the first commit
     * of every new commit blob. So the commit blob that holds a commit has a copy of every generational file of the commit. A commit
     * carries a generational file when the file first appeared in an older commit blob.
     * <p>
     * {@link StatelessCommitService#getBlobLocation} returns the first location that the commit service learned for a file. For a
     * generational file that is the oldest copy that this primary knows: in the commit blob where the file first appeared, or after a
     * recovery in the commit blob of the recovered commit. A snapshot copies whole commit blobs. So a file info that points at the old
     * copy keeps the copy of the old commit blob in the repository for as long as any snapshot lists the file info, even when the
     * snapshot needs no other file from that commit blob. A later snapshot gets a new file info that points at the copy in its own,
     * newer commit blob. A snapshot delete can then remove the copy of the older commit blob once no remaining snapshot needs another
     * file in it.
     * <p>
     * The method handles these cases:
     * <ul>
     * <li>The commit carries no generational file: the method returns the locations as they are, and reads nothing.</li>
     * <li>The commit carries generational files: the method sends one GET for the start of the commit blob that holds the commit. The
     * start of a commit blob is the header of its first commit. That header lists a copy of every generational file that was in use
     * when the commit blob was started, and a generational file that the commit carries was in use then. The method takes the location
     * of each carried file from that header. A generational file that a commit wrote into the same commit blob keeps its location,
     * because its first location is in that commit blob already. The method reads the first header, and not the header of the commit
     * itself, because the first header starts at byte 0 of the blob. The header of a later commit starts after the commits before it,
     * and finding it would take one more GET for each of them.</li>
     * <li>The header has no copy of a carried file in that commit blob, or the copy has another length, or the read fails: the method
     * throws {@link IndexShardSnapshotFailedException}, and the shard snapshot fails. The method does not fall back to the first
     * location. A header without the copy means that the commit blobs are not laid out as this code relies on, so the snapshot fails
     * instead of guessing.</li>
     * </ul>
     * The caller must hold the commit while it calls this method, so that the commit blob stays in the object store. The method cannot
     * know whether the snapshot will copy a commit blob. So the method also reads the header when the repository reuses the file list
     * of an earlier snapshot of an unchanged shard, and when server-side copy is off. In both cases the read is not needed.
     */
    private Map<String, BlobLocation> withGenerationalFilesInOwnCommitBlob(
        ShardId shardId,
        IndexCommit indexCommit,
        Map<String, BlobLocation> firstLocations
    ) {
        if (firstLocations.keySet().stream().noneMatch(StatelessCompoundCommit::isGenerationalFile)) {
            return firstLocations;
        }
        // The commit writes its segments_N file into its own commit blob, and into no other blob. So the location of that file says
        // which commit blob holds the commit.
        final BlobLocation segmentsFile = firstLocations.get(indexCommit.getSegmentsFileName());
        final BlobFile ownBlob = segmentsFile.blobFile();
        final List<String> carriedFiles = firstLocations.entrySet()
            .stream()
            .filter(
                entry -> StatelessCompoundCommit.isGenerationalFile(entry.getKey()) && entry.getValue().blobFile().equals(ownBlob) == false
            )
            .map(Map.Entry::getKey)
            .toList();
        if (carriedFiles.isEmpty()) {
            return firstLocations;
        }
        final StatelessCompoundCommit firstCommit = readFirstCommitHeader(shardId, segmentsFile);
        final Map<String, BlobLocation> locations = new HashMap<>(firstLocations);
        for (String file : carriedFiles) {
            final BlobLocation copy = firstCommit.commitFiles().get(file);
            if (copy == null || copy.blobFile().equals(ownBlob) == false || copy.fileLength() != firstLocations.get(file).fileLength()) {
                throw new IndexShardSnapshotFailedException(
                    shardId,
                    "the first commit header of commit blob ["
                        + ownBlob.blobName()
                        + "] has no copy of generational file ["
                        + file
                        + "] of length ["
                        + firstLocations.get(file).fileLength()
                        + "] in that blob, but lists "
                        + copy
                );
            }
            locations.put(file, copy);
        }
        return Map.copyOf(locations);
    }

    /**
     * Reads the header of the first commit of the commit blob that holds the {@code segments_N} file at the given location, with one
     * GET. {@code segments_N} is the Lucene file that lists the segments of one commit, and a commit stores it in its own commit blob.
     * The first commit starts at byte 0 of the blob. The {@code segments_N} file of the first commit, or of a later commit, comes after
     * the whole first header. So the GET asks for the bytes before the {@code segments_N} file. The method closes the stream as soon as
     * it has the header, and does not read the remaining bytes of the GET.
     */
    private StatelessCompoundCommit readFirstCommitHeader(ShardId shardId, BlobLocation segmentsFile) {
        final BlobContainer container = objectStoreService.getProjectBlobContainer(shardId, segmentsFile.primaryTerm());
        try (
            var input = new InputStreamStreamInput(
                container.readBlob(OperationPurpose.SNAPSHOT_METADATA, segmentsFile.blobName(), 0, segmentsFile.offset())
            )
        ) {
            return StatelessCompoundCommit.readFromStoreAtOffset(input, 0, generation -> segmentsFile.compoundFileGeneration());
        } catch (IOException e) {
            throw new IndexShardSnapshotFailedException(
                shardId,
                "failed to read the first commit header of commit blob [" + segmentsFile.blobName() + "]",
                e
            );
        }
    }

    /**
     * Register a releasable for the shard snapshot and the commit it uses. When the releasable closes, the commit is released.
     * This class will always release the specified SnapshotIndexCommit at some point in the future, e.g., shard snapshot completion
     * and shard close. Therefore, the caller is not responsible for the release, i.e., the ownership is transferred to this
     * class.
     */
    // Package private for testing
    void registerReleaseForSnapshot(ShardId shardId, Snapshot snapshot, SnapshotIndexCommit snapshotIndexCommit) {
        final var releasable = Releasables.releaseOnce(snapshotIndexCommit::releaseInitialReference);

        final var old = new Releasable[1];
        final var shardReleasables = snapshotsCommitReleasables.compute(shardId, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            old[0] = v.put(snapshot, releasable);
            return v;
        });
        Releasables.close(old); // close any old releasable that maybe created on recovery or previous register attempt

        // The shard may concurrently close right before the above releasable is added, check the shard closed status again to repair
        if (commitService.isShardClosed(shardId)) {
            logger.debug("shard {} concurrently closed while registering snapshot [{}] commit releasable", shardId, snapshot);
            releaseCommitsAndRemoveShardAfterShardClosed(shardId);
        } else if (isShardSnapshotReadingData(SnapshotsInProgress.get(clusterService.state()).snapshot(snapshot), shardId) == false) {
            // If the snapshotting node is a remote node and lagging, master can remove it and concurrently mark the shard snapshot
            // as FAILED during the registration attempt.
            logger.debug("shard {} snapshot [{}] concurrently completed while registering commit releasable", shardId, snapshot);
            Releasables.close(shardReleasables.remove(snapshot));
            if (shardReleasables.isEmpty()) {
                removeShardIfNoTrackedSnapshot(shardId);
            }
            throw new IndexShardSnapshotFailedException(shardId, "snapshot [" + snapshot + "] is no longer active");
        }
    }

    /**
     * Callback on shard closed. Remove the shard from tracking and release all its retained commits for snapshots
     * if the index is not being deleted. When the shard is closing as part of an index deletion, tracking is retained
     * so the commit blobs stay alive until each snapshot gets aborted, see also SnapshotShardsService. This prevents
     * NoSuchFileException being thrown during shard snapshots, especially when the shard runs on a different node
     * than the shard snapshot. The retained releasables are closed instead by {@link #clusterChanged} once shard snapshots
     * are no longer reading shard data.
     */
    public void releaseCommitsAndRemoveShardAfterShardClosed(ShardId shardId) {
        assert commitService.isShardClosed(shardId) : "shard [" + shardId + "] is not closed";
        if (commitService.isShardDeletingIndex(shardId)) {
            logger.debug("retain snapshot commits for shard {} after shard close due to index deletion", shardId);
            return;
        }
        final var shardReleasables = snapshotsCommitReleasables.remove(shardId);
        if (shardReleasables != null) {
            logger.debug("release snapshot commits for shard {} after shard close", shardId);
            shardRelocationDuringSnapshotCounter.increment();
            Releasables.close(shardReleasables.values());
        }
    }

    /**
     * For each running shard snapshot for the given shard, creates a consumer that expects to receive a releasable.
     * When the releasable is closed, it dec-refs on all commits that were inc-refed for creating it.
     * It is used as a callback at recovery time to add additional refcounts to pre-recovery commits.
     */
    public Iterator<Consumer<RecoveredCommitsReleasable>> getExtraReferenceConsumers(ShardId shardId) {
        return Iterators.map(getRunningSnapshots(shardId), snapshot -> originalReleasable -> {
            recoveryRetainedCommitsHistogram.record(originalReleasable.size());
            final long startTime = timeProvider.relativeTimeInMillis();
            final var releasable = Releasables.releaseOnce(
                Releasables.wrap(
                    originalReleasable,
                    () -> recoveryRetainedDurationHistogram.record((timeProvider.relativeTimeInMillis() - startTime) / 1_000d)
                )
            );

            logger.debug("{} conservatively retaining commits for running snapshot [{}]", shardId, snapshot);
            final var shardReleasables = snapshotsCommitReleasables.compute(shardId, (k, v) -> {
                if (v == null) {
                    v = new ConcurrentHashMap<>();
                }
                final var old = v.put(snapshot, releasable);
                assert old == null : "shard " + shardId + " has unexpected tracking for snapshot " + snapshot + " before recovery";
                return v;
            });

            // The snapshot may concurrently complete while the shard is recovering right before the above releasable is added.
            // So we check snapshot completion again to repair for it.
            final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            if (isShardSnapshotReadingData(snapshotsInProgress.snapshot(snapshot), shardId) == false) {
                logger.debug("shard {} snapshot [{}] concurrently completed while retaining commits during recovery", shardId, snapshot);
                Releasables.close(shardReleasables.remove(snapshot));
                if (shardReleasables.isEmpty()) {
                    removeShardIfNoTrackedSnapshot(shardId);
                }
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Iterate through all tracked snapshots and release those that no longer actively reading shard data
        final var snapshotsInProgress = SnapshotsInProgress.get(event.state());
        if (snapshotsInProgress.equals(SnapshotsInProgress.get(event.previousState())) == false) {
            // ConcurrentHashMap's iterator is not strongly consistent. That's OK here since we don't need it here either.
            snapshotsCommitReleasables.forEach((shardId, shardReleasables) -> {
                shardReleasables.forEach((snapshot, releasable) -> {
                    if (isShardSnapshotReadingData(snapshotsInProgress.snapshot(snapshot), shardId) == false) {
                        // Be paranoid and remove snapshot if it still has the same releasable
                        if (shardReleasables.remove(snapshot, releasable)) {
                            Releasables.close(releasable);
                        }
                    }
                });
                if (shardReleasables.isEmpty()) {
                    removeShardIfNoTrackedSnapshot(shardId);
                }
            });
        }
    }

    private void removeShardIfNoTrackedSnapshot(ShardId shardId) {
        snapshotsCommitReleasables.compute(shardId, (k, v) -> v == null ? null : (v.isEmpty() ? null : v));
    }

    private Iterator<Snapshot> getRunningSnapshots(ShardId shardId) {
        return Iterators.flatMap(
            SnapshotsInProgress.get(clusterService.state()).entriesByRepo().iterator(),
            entries -> Iterators.map(
                Iterators.filter(entries.iterator(), entry -> isShardSnapshotReadingData(entry, shardId)),
                SnapshotsInProgress.Entry::snapshot
            )
        );
    }

    private static boolean isShardSnapshotReadingData(SnapshotsInProgress.Entry snapshotEntry, ShardId shardId) {
        if (snapshotEntry == null || snapshotEntry.isClone() || snapshotEntry.state().completed()) {
            return false;
        }
        final var shardSnapshotStatus = snapshotEntry.shards().get(shardId);
        // Both INIT and ABORTED shard snapshot may need to read existing shard data
        return shardSnapshotStatus != null
            && (shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.INIT
                || shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.ABORTED);
    }

    // package private for testing only
    boolean hasTrackingForShard(ShardId shardId) {
        return snapshotsCommitReleasables.containsKey(shardId);
    }

    // package private for testing only
    void assertEmptyTracking() {
        assert snapshotsCommitReleasables.isEmpty() : "tracking is not empty: " + snapshotsCommitReleasables;
    }

    /**
     * A {@link BooleanSupplier} that latches to {@code true} once the wrapped delegate first returns {@code true}, and then nullifies
     * the delegate for garbage collection, e.g. allowing an {@link IndexShard} become eligible for GC once it is relocated.
     */
    static final class LatchingBooleanSupplier implements BooleanSupplier {
        private volatile BooleanSupplier delegate;
        private boolean latched; // no need for volatile since its read and write are fenced by the volatile delegate

        LatchingBooleanSupplier(BooleanSupplier delegate) {
            this.latched = Objects.requireNonNull(delegate).getAsBoolean();
            this.delegate = this.latched ? null : delegate;
        }

        @Override
        public boolean getAsBoolean() {
            final var d = delegate;
            if (d == null) {
                assert latched : "delegate can only be null when latched is true";
                return true;
            }
            final boolean result = d.getAsBoolean();
            if (result) {
                latched = true;
                delegate = null; // nullify to release the delegate for GC
            }
            return result;
        }

        // Package-private for testing
        BooleanSupplier delegate() {
            return delegate;
        }
    }
}
