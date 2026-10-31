/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.LatestCommit;

import java.io.IOException;

/**
 * Takes the latest commit of a primary shard on this node, as {@link ShardBaselineCache} needs it: the commit's files, with their
 * lengths and writer uuids, and the state id a snapshot would give it. The shard isn't flushed first, so writes that aren't in a
 * commit yet aren't counted.
 */
record IndexShardLatestCommits(IndexShard shard) implements ShardBaselineCache.LatestCommits {

    @Override
    public LatestCommit get() throws IOException {
        try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(false)) {
            return latestCommit(shard, commitRef.getIndexCommit(), shard.withEngine(Engine::getLastCommittedSegmentInfos));
        }
    }

    /**
     * The files come from the commit's segment infos. The engine keeps those of its last commit in memory, so they are used when they
     * are the commit's. If a newer commit landed after the commit was taken, they aren't, and the commit's own are read instead,
     * which also reads each segment's {@code .si} file.
     *
     * @param lastCommittedSegmentInfos the segment infos of the engine's last commit
     */
    static LatestCommit latestCommit(IndexShard shard, IndexCommit commit, SegmentInfos lastCommittedSegmentInfos) throws IOException {
        final SegmentInfos segmentInfos = lastCommittedSegmentInfos.getSegmentsFileName().equals(commit.getSegmentsFileName())
            ? lastCommittedSegmentInfos
            : Lucene.readSegmentInfos(commit);
        return new LatestCommit(
            SnapshotShardsService.getShardStateId(shard, commit),
            ShardBaseline.luceneFiles(segmentInfos, commit.getDirectory())
        );
    }
}
