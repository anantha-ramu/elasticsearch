/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

/**
 * Keeps a {@link ShardBaseline} for each primary shard on this node and each repository it is measured for, and uses them to work
 * out each shard's snapshot backlog: the bytes a snapshot of its latest commit would upload ({@link #measure}).
 * <p>
 * Building a baseline means reading the shard's backup metadata ({@code index-{generation}}) from the repository, so it happens in
 * the background, at most {@link #MAX_CONCURRENT_LOADS} at a time. Measuring reads nothing from the repository: it takes the shard's
 * latest commit and asks the baseline. Until there is a baseline for the generation the caller asks about, the shard is reported as
 * not measured, with the reason. Each measured figure carries the generation it was measured against.
 * <p>
 * How each case is handled:
 * <ul>
 *     <li><b>The generation changes</b>, because a snapshot was taken, deleted or cloned. The old baseline is dropped at once, because a
 *     baseline built from older backup metadata can count too little after a deletion, and a new one is loaded. Until it is built, the
 *     shard is not measured. A load still waiting in the queue reads the newest generation when it runs, and a load that finishes for a
 *     generation that has since changed is thrown away and the new generation is loaded straight away.</li>
 *     <li><b>A load fails</b>, for example because the repository can't be reached, or because a newer generation replaced the one
 *     asked for. The shard is not measured, and it isn't loaded again for 30 seconds, then twice as long after each further failure,
 *     up to 10 minutes. There are no timers: the retry starts on the first {@link #measure} after the delay. A new generation is
 *     loaded at once, whatever the delay. Each failure is logged at debug level. A repository that fails also gets at most one warning
 *     every 10 minutes; a shard whose latest commit can't be taken, for example because it is closing, doesn't make one.</li>
 *     <li><b>The memory budget is used up.</b> A shard copy that has never had a baseline is refused and not measured, and is tried
 *     again with the same delays as a failure. A shard copy that has one always gets its rebuild after a generation change, so a node
 *     near its budget can't flip shards in and out. Nothing is evicted.</li>
 *     <li><b>A shard leaves the node</b> ({@link #retainShards}), or another copy of it takes its place on the node. Its baselines are
 *     dropped, and a load still queued or running for it changes nothing when it finishes.</li>
 *     <li><b>Measuring is turned off</b> ({@link #close}). Everything is dropped, and loads still queued or running change nothing.</li>
 * </ul>
 * A load reads the backup metadata one entry at a time, keeping only what the baseline needs ({@link ShardBaselineFormat}), so it
 * takes about as much memory as the baseline it builds, however many snapshots the repository keeps.
 */
final class ShardBaselineCache implements Accountable, Releasable {

    private static final Logger logger = LogManager.getLogger(ShardBaselineCache.class);

    static final int MAX_CONCURRENT_LOADS = 4;
    static final TimeValue FIRST_RETRY_DELAY = TimeValue.timeValueSeconds(30);
    static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueMinutes(10);
    static final TimeValue WARNING_INTERVAL = TimeValue.timeValueMinutes(10);

    /**
     * One copy of a shard on this node. Another copy of the same shard, for example after it relocated away and back, has another
     * allocation id, and starts with no baselines.
     */
    record ShardCopy(ShardId shardId, String allocationId) {}

    /**
     * The backlog of a shard for a repository, or why it isn't known.
     */
    sealed interface Result permits Measured, NotMeasured {}

    /**
     * The bytes a snapshot of the shard's latest commit would upload, measured against the given generation of its backup metadata.
     */
    record Measured(ShardGeneration generation, long bytes) implements Result {}

    record NotMeasured(Reason reason) implements Result {}

    enum Reason {
        /** The baseline for the generation asked about is being loaded. */
        LOADING,
        /** Reading the backup metadata, or the shard's latest commit, failed. */
        READ_FAILED,
        /** The shard copy has no baseline yet, and building one would go over the memory budget. */
        OVER_BUDGET,
        /** Measuring is turned off. */
        CLOSED
    }

    /**
     * Reads a shard's backup metadata from a repository straight into a baseline, see {@link ShardBaselineFormat}.
     */
    interface MetadataReader {
        /**
         * Reads the blob of exactly this generation, and fails if it doesn't exist. Never the latest blob, or a blob found by listing:
         * the generation was published after its blob was written in full, which is what makes the commit taken before the read
         * right (see {@link ShardBaseline#of}).
         *
         * @param currentFiles the files of the shard's build commit, see {@link ShardBaseline#of}
         */
        ShardBaseline read(ProjectRepo repository, IndexId indexId, int shardId, ShardGeneration generation, List<LuceneFile> currentFiles)
            throws IOException;
    }

    /**
     * Takes the latest commit of a shard.
     */
    interface LatestCommits {
        LatestCommit get() throws IOException;
    }

    /**
     * A shard's latest commit: the state id a snapshot would give it, or {@code null} while writes are in flight, and its files.
     */
    record LatestCommit(@Nullable String stateId, List<LuceneFile> files) {}

    private record Key(ProjectRepo repository, ShardId shardId) {}

    // What the cache knows about one shard copy for one repository.
    private static final class Entry {
        final String allocationId;
        // The generation and source of the latest measure, which the next load uses.
        ShardGeneration generation;
        IndexId indexId;
        LatestCommits commits;
        // The baseline for the generation, once loaded.
        @Nullable
        ShardBaseline baseline;
        // Whether a load is queued or running.
        boolean loading;
        // Why the last load for the generation failed, and when it may be tried again.
        @Nullable
        Reason failure;
        long retryDelayMillis;
        long retryAtMillis;
        // Whether the shard copy ever got a baseline, which the memory budget never takes away again.
        boolean admitted;

        Entry(String allocationId, ShardGeneration generation) {
            this.allocationId = allocationId;
            this.generation = generation;
        }
    }

    // The load failures of one repository since its last warning.
    private static final class Warnings {
        long nextWarningMillis = Long.MIN_VALUE;
        int failures;
    }

    private final MetadataReader reader;
    private final Executor loadExecutor;
    private final LongSupplier relativeTimeMillis;
    private final long budgetBytes;

    // All of these are guarded by this object's lock. The reads run outside it.
    private final Map<Key, Entry> entries = new HashMap<>();
    private final Map<ProjectRepo, Warnings> warnings = new HashMap<>();
    private long baselineBytes;
    private boolean closed;

    /**
     * @param executor           runs the loads, which read from the repository
     * @param relativeTimeMillis the clock for the delays between retries and between warnings
     * @param budgetBytes        how much memory the baselines may use, see {@link #memoryBudget}
     */
    ShardBaselineCache(MetadataReader reader, Executor executor, LongSupplier relativeTimeMillis, long budgetBytes) {
        this.reader = reader;
        this.loadExecutor = new ThrottledTaskRunner("snapshot-backlog-baselines", MAX_CONCURRENT_LOADS, executor).asExecutor();
        this.relativeTimeMillis = relativeTimeMillis;
        this.budgetBytes = budgetBytes;
    }

    /**
     * The memory budget for the baselines: 1% of the heap. It guards against surprises rather than being expected to bind: from
     * production shard and segment counts, the largest index node has about 64,000 current files, which at about 56 bytes per file
     * plus its name comes to about 6 MB, under 0.1% of its heap.
     */
    static long memoryBudget(long maxHeapBytes) {
        return maxHeapBytes / 100;
    }

    /**
     * The snapshot backlog of a shard copy for a repository, measured against the given generation of the shard's backup metadata, or
     * why it can't be measured yet. If there is no baseline for this generation, this starts loading one, unless a load is already
     * queued or the shard is waiting to retry, and answers "not measured".
     * <p>
     * Measuring reads nothing from the repository, but taking the latest commit may read the commit's segment infos from disk, so this
     * must run on a thread that may block.
     *
     * @param generation the generation the next snapshot of the shard would start from. It must come from the master's
     *                   {@code RepositoryData} or from {@code SnapshotsInProgress}, which publish a generation only after its blob is
     *                   written in full: loads take the shard's commit before reading the blob, which is only right for such a
     *                   generation (see {@link ShardBaseline#of})
     * @param commits    takes the shard's latest commit
     */
    Result measure(ProjectRepo repository, ShardCopy shard, IndexId indexId, ShardGeneration generation, LatestCommits commits) {
        assert Transports.assertNotTransportThread("measuring the snapshot backlog may read from disk");
        assert ShardGenerations.DELETED_SHARD_GEN.equals(generation) == false : generation;
        final ShardBaseline baseline;
        synchronized (this) {
            if (closed) {
                return new NotMeasured(Reason.CLOSED);
            }
            final Key key = new Key(repository, shard.shardId());
            Entry entry = entries.get(key);
            if (entry == null || entry.allocationId.equals(shard.allocationId()) == false) {
                if (entry != null) {
                    dropBaseline(entry);
                }
                entry = new Entry(shard.allocationId(), generation);
                entries.put(key, entry);
            } else if (entry.generation.equals(generation) == false) {
                // A baseline built from older backup metadata can count too little after a deletion, so it is never used again.
                dropBaseline(entry);
                entry.generation = generation;
                entry.failure = null;
                entry.retryDelayMillis = 0;
            }
            entry.indexId = indexId;
            entry.commits = commits;
            if (entry.baseline == null) {
                maybeStartLoad(key, entry);
                return new NotMeasured(entry.failure == null ? Reason.LOADING : entry.failure);
            }
            baseline = entry.baseline;
        }
        final LatestCommit commit;
        try {
            commit = commits.get();
        } catch (Exception e) {
            logCommitFailure(new Key(repository, shard.shardId()), e);
            return new NotMeasured(Reason.READ_FAILED);
        }
        return new Measured(generation, baseline.bytesToUpload(commit.stateId(), commit.files()));
    }

    /**
     * Drops everything about the shard copies on this node that are not in {@code shards}. Call it whenever a shard closes, too: until
     * then, the cache keeps the shard's {@link LatestCommits}, and with it a reference to the shard.
     */
    synchronized void retainShards(Set<ShardCopy> shards) {
        entries.entrySet().removeIf(keyAndEntry -> {
            if (shards.contains(new ShardCopy(keyAndEntry.getKey().shardId(), keyAndEntry.getValue().allocationId))) {
                return false;
            }
            dropBaseline(keyAndEntry.getValue());
            return true;
        });
    }

    /**
     * Drops everything and stops: loads still queued or running change nothing, and {@link #measure} answers "not measured".
     */
    @Override
    public synchronized void close() {
        closed = true;
        entries.clear();
        warnings.clear();
        baselineBytes = 0;
    }

    /**
     * The memory the baselines use.
     */
    @Override
    public synchronized long ramBytesUsed() {
        return baselineBytes;
    }

    private void maybeStartLoad(Key key, Entry entry) {
        assert Thread.holdsLock(this);
        if (entry.loading || (entry.failure != null && relativeTimeMillis.getAsLong() < entry.retryAtMillis)) {
            return;
        }
        if (entry.admitted == false && baselineBytes >= budgetBytes) {
            recordFailure(entry, Reason.OVER_BUDGET);
            return;
        }
        entry.loading = true;
        // While the retry runs, the shard is loading, not failed.
        entry.failure = null;
        loadExecutor.execute(() -> load(key, entry));
    }

    private void load(Key key, Entry entry) {
        final ShardGeneration generation;
        final IndexId indexId;
        final LatestCommits commits;
        synchronized (this) {
            if (isCurrent(key, entry) == false) {
                return;
            }
            // Taken now, not when the load was queued, so that a load that waited reads the newest generation.
            generation = entry.generation;
            indexId = entry.indexId;
            commits = entry.commits;
        }
        // The commit is taken now, after the caller learned the generation, so every snapshot in that generation's metadata is of a
        // commit at or before it, as ShardBaseline#of requires.
        LatestCommit commit = null;
        Exception commitFailure = null;
        try {
            commit = commits.get();
        } catch (Exception e) {
            commitFailure = e;
        }
        ShardBaseline baseline = null;
        Exception readFailure = null;
        if (commit != null) {
            try {
                baseline = ShardGenerations.NEW_SHARD_GEN.equals(generation)
                    ? ShardBaseline.of(BlobStoreIndexShardSnapshots.EMPTY, commit.files())
                    : reader.read(key.repository(), indexId, key.shardId().id(), generation, commit.files());
            } catch (Exception e) {
                readFailure = e;
            }
        }
        synchronized (this) {
            if (isCurrent(key, entry) == false) {
                return;
            }
            entry.loading = false;
            if (entry.generation.equals(generation) == false) {
                // The generation changed while this load ran, so its result is already out of date. Load the new one now, rather than
                // on the next measure.
                maybeStartLoad(key, entry);
            } else if (readFailure != null) {
                recordFailure(entry, Reason.READ_FAILED);
                logReadFailure(key, generation, readFailure);
            } else if (commitFailure != null) {
                // For example, the shard is closing. That isn't the repository's fault, so it isn't warned about.
                recordFailure(entry, Reason.READ_FAILED);
                logCommitFailure(key, commitFailure);
            } else if (entry.admitted == false && baselineBytes + baseline.ramBytesUsed() > budgetBytes) {
                recordFailure(entry, Reason.OVER_BUDGET);
            } else {
                entry.admitted = true;
                entry.baseline = baseline;
                entry.retryDelayMillis = 0;
                baselineBytes += baseline.ramBytesUsed();
            }
        }
    }

    // Whether the entry is still the one for its shard copy: not dropped because the shard left, another copy came, or close().
    private boolean isCurrent(Key key, Entry entry) {
        assert Thread.holdsLock(this);
        return entries.get(key) == entry;
    }

    private void dropBaseline(Entry entry) {
        assert Thread.holdsLock(this);
        if (entry.baseline != null) {
            baselineBytes -= entry.baseline.ramBytesUsed();
            entry.baseline = null;
        }
    }

    // Not measured for this reason, and not loaded again until the delay, which doubles with each failure in a row.
    private void recordFailure(Entry entry, Reason reason) {
        assert Thread.holdsLock(this);
        entry.failure = reason;
        entry.retryDelayMillis = entry.retryDelayMillis == 0
            ? FIRST_RETRY_DELAY.millis()
            : Math.min(2 * entry.retryDelayMillis, MAX_RETRY_DELAY.millis());
        entry.retryAtMillis = relativeTimeMillis.getAsLong() + entry.retryDelayMillis;
    }

    // Logs every failure to read the backup metadata at debug level, and warns at most once per repository per interval.
    private void logReadFailure(Key key, ShardGeneration generation, Exception e) {
        assert Thread.holdsLock(this);
        logger.debug(
            () -> Strings.format("[%s] failed to load the backup metadata of %s at %s", key.repository(), key.shardId(), generation),
            e
        );
        final long now = relativeTimeMillis.getAsLong();
        final Warnings repositoryWarnings = warnings.computeIfAbsent(key.repository(), repository -> new Warnings());
        repositoryWarnings.failures += 1;
        if (now >= repositoryWarnings.nextWarningMillis) {
            logger.warn(
                "[{}] failed to load the backup metadata of {} shard(s) since the last warning; the latest failure, for {}, was [{}]",
                key.repository(),
                repositoryWarnings.failures,
                key.shardId(),
                e.getMessage()
            );
            repositoryWarnings.failures = 0;
            repositoryWarnings.nextWarningMillis = now + WARNING_INTERVAL.millis();
        }
    }

    private static void logCommitFailure(Key key, Exception e) {
        logger.debug(() -> Strings.format("[%s] cannot take the latest commit of %s", key.repository(), key.shardId()), e);
    }
}
