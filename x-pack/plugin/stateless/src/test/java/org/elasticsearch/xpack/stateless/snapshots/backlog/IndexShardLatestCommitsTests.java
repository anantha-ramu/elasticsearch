/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.LatestCommit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Checks that {@link IndexShardLatestCommits} gives the files and state id a snapshot would read for the same commit
 * ({@link Store.MetadataSnapshot#loadFromIndexCommit} and {@link SnapshotShardsService#getShardStateId}).
 */
public class IndexShardLatestCommitsTests extends IndexShardTestCase {

    public void testTheLatestCommitIsReadAsASnapshotReadsIt() throws IOException {
        final IndexShard shard = newStartedShard(true);
        try {
            indexDocsAndFlush(shard);
            if (randomBoolean()) {
                deleteDoc(shard, "0");
                flushShard(shard, true);
            }

            final LatestCommit latest = new IndexShardLatestCommits(shard).get();

            try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(false)) {
                assertCommit(shard, commitRef.getIndexCommit(), latest);
            }
        } finally {
            closeShards(shard);
        }
    }

    public void testACommitOlderThanTheEnginesLastReadsItsOwnSegmentInfos() throws IOException {
        final IndexShard shard = newStartedShard(true);
        try {
            indexDocsAndFlush(shard);
            try (Engine.IndexCommitRef olderCommit = shard.acquireLastIndexCommit(false)) {
                // A new commit lands after the older one was taken.
                indexDocsAndFlush(shard);
                final SegmentInfos lastCommitted = shard.withEngine(Engine::getLastCommittedSegmentInfos);
                assertThat(lastCommitted.getSegmentsFileName(), not(equalTo(olderCommit.getIndexCommit().getSegmentsFileName())));

                final LatestCommit latest = IndexShardLatestCommits.latestCommit(shard, olderCommit.getIndexCommit(), lastCommitted);

                assertCommit(shard, olderCommit.getIndexCommit(), latest);
            }
        } finally {
            closeShards(shard);
        }
    }

    private void indexDocsAndFlush(IndexShard shard) throws IOException {
        for (int i = between(1, 20); i > 0; i--) {
            indexDoc(shard, "_doc", randomAlphaOfLength(10));
        }
        indexDoc(shard, "_doc", "0");
        flushShard(shard, true);
    }

    private void assertCommit(IndexShard shard, IndexCommit commit, LatestCommit latest) throws IOException {
        assertThat(latest.stateId(), equalTo(SnapshotShardsService.getShardStateId(shard, commit)));
        final Set<LuceneFile> expected = new HashSet<>();
        for (StoreFileMetadata file : Store.MetadataSnapshot.loadFromIndexCommit(commit, commit.getDirectory(), logger)) {
            expected.add(new LuceneFile(file.name(), file.length(), file.writerUuid()));
        }
        assertThat(latest.files().size(), equalTo(expected.size()));
        assertThat(Set.copyOf(latest.files()), equalTo(expected));
    }
}
