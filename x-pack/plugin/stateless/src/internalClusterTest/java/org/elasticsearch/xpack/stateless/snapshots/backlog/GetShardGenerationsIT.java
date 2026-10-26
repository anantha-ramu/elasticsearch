/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.createFullSnapshot;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.getRepositoryData;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.getRepositoryOnMaster;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Asks a real master from an index node, and checks the answers against the repository data the master has.
 */
public class GetShardGenerationsIT extends AbstractStatelessPluginIntegTestCase {

    private static final String REPOSITORY = "repo";
    private static final ProjectRepo DEFAULT_PROJECT_REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, REPOSITORY);
    private static final String INDEX = "index";

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    /**
     * Snapshots as in serverless: read from the object store, and kept running while a shard relocates.
     */
    private static Settings snapshotNodeSettings() {
        return Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private static GetShardGenerationsResponse getShardGenerations(String node, ProjectRepo projectRepo, List<ShardId> shardIds) {
        return safeGet(
            client(node).execute(
                TransportGetShardGenerationsAction.TYPE,
                new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, projectRepo, shardIds)
            )
        );
    }

    private static Throwable getShardGenerationsFailure(String node, ProjectRepo projectRepo, List<ShardId> shardIds) {
        return ExceptionsHelper.unwrapCause(
            safeAwaitFailure(
                GetShardGenerationsResponse.class,
                listener -> client(node).execute(
                    TransportGetShardGenerationsAction.TYPE,
                    new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, projectRepo, shardIds),
                    listener
                )
            )
        );
    }

    /**
     * @return what the master's repository data says about the shards, in the form the action answers with
     */
    private static Map<ShardId, RepositoryShardGeneration> expectedShardGenerations(List<ShardId> shardIds) {
        final RepositoryData repositoryData = getRepositoryData(getRepositoryOnMaster(REPOSITORY));
        final Map<ShardId, RepositoryShardGeneration> expected = new HashMap<>();
        for (ShardId shardId : shardIds) {
            final var indexId = repositoryData.resolveIndexId(shardId.getIndexName());
            expected.put(
                shardId,
                new RepositoryShardGeneration(indexId, repositoryData.shardGenerations().getShardGen(indexId, shardId.id()))
            );
        }
        return expected;
    }

    public void testIndexNodeGetsTheShardGenerationsOfTheMaster() {
        startMasterOnlyNode(snapshotNodeSettings());
        final String indexNode = startIndexNode(snapshotNodeSettings());
        createRepository(REPOSITORY, "fs");
        final int numberOfShards = between(1, 3);
        createIndex(INDEX, indexSettings(numberOfShards, 0).build());
        ensureGreen(INDEX);
        indexDocs(INDEX, between(10, 100));
        final var index = resolveIndex(INDEX);
        final List<ShardId> shardIds = new ArrayList<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardIds.add(new ShardId(index, i));
        }

        // before the first snapshot the repository holds nothing of the shards
        final var beforeSnapshot = getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds);
        assertThat(beforeSnapshot.repositoryGeneration(), equalTo(getRepositoryData(getRepositoryOnMaster(REPOSITORY)).getGenId()));
        assertThat(beforeSnapshot.shardGenerations(), anEmptyMap());

        createFullSnapshot(logger, REPOSITORY, "snap-1");
        final var afterFirstSnapshot = getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds);
        assertThat(afterFirstSnapshot.repositoryGeneration(), equalTo(getRepositoryData(getRepositoryOnMaster(REPOSITORY)).getGenId()));
        assertThat(afterFirstSnapshot.shardGenerations(), equalTo(expectedShardGenerations(shardIds)));

        // every snapshot writes new shard-level metadata for every shard it includes, even when nothing was written in between
        createFullSnapshot(logger, REPOSITORY, "snap-2");
        final var afterSecondSnapshot = getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds);
        assertThat(afterSecondSnapshot.shardGenerations(), equalTo(expectedShardGenerations(shardIds)));
        for (ShardId shardId : shardIds) {
            assertThat(
                afterSecondSnapshot.shardGenerations().get(shardId).generation(),
                not(equalTo(afterFirstSnapshot.shardGenerations().get(shardId).generation()))
            );
        }

        // deleting a snapshot (as retention does) writes new shard-level metadata for every shard of it as well
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, "snap-1"));
        final var afterFirstDelete = getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds);
        assertThat(afterFirstDelete.repositoryGeneration(), equalTo(getRepositoryData(getRepositoryOnMaster(REPOSITORY)).getGenId()));
        assertThat(afterFirstDelete.shardGenerations(), equalTo(expectedShardGenerations(shardIds)));
        for (ShardId shardId : shardIds) {
            assertThat(
                afterFirstDelete.shardGenerations().get(shardId).generation(),
                not(equalTo(afterSecondSnapshot.shardGenerations().get(shardId).generation()))
            );
        }

        // once no snapshot refers to the index the repository holds nothing of the shards any more
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, "snap-2"));
        final var afterDelete = getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds);
        assertThat(afterDelete.repositoryGeneration(), equalTo(getRepositoryData(getRepositoryOnMaster(REPOSITORY)).getGenId()));
        assertThat(afterDelete.shardGenerations(), anEmptyMap());
    }

    public void testUnknownRepository() {
        startMasterOnlyNode(snapshotNodeSettings());
        final String indexNode = startIndexNode(snapshotNodeSettings());
        createIndex(INDEX, indexSettings(1, 0).build());
        ensureGreen(INDEX);

        final var shardIds = List.of(new ShardId(resolveIndex(INDEX), 0));
        final var e = getShardGenerationsFailure(indexNode, new ProjectRepo(ProjectId.DEFAULT, "missing"), shardIds);
        assertThat(e.getClass(), equalTo(RepositoryMissingException.class));
    }

    /**
     * The repository is looked up in the project the request names, not by its name alone. These tests run one project only, so this
     * asks for the repository of the default project under the id of a project that does not exist.
     */
    public void testRepositoryOfAnotherProjectIsNotUsed() {
        startMasterOnlyNode(snapshotNodeSettings());
        final String indexNode = startIndexNode(snapshotNodeSettings());
        createRepository(REPOSITORY, "fs");
        createIndex(INDEX, indexSettings(1, 0).build());
        ensureGreen(INDEX);
        indexDocs(INDEX, between(10, 100));
        createFullSnapshot(logger, REPOSITORY, "snap");
        final var shardIds = List.of(new ShardId(resolveIndex(INDEX), 0));
        // the default project's repository has shard-level metadata of the shard
        assertThat(
            getShardGenerations(indexNode, DEFAULT_PROJECT_REPOSITORY, shardIds).shardGenerations().keySet(),
            equalTo(Set.copyOf(shardIds))
        );

        final var otherProject = randomValueOtherThan(ProjectId.DEFAULT, ESTestCase::randomUniqueProjectId);
        final var e = getShardGenerationsFailure(indexNode, new ProjectRepo(otherProject, REPOSITORY), shardIds);
        assertThat(e.getClass(), equalTo(RepositoryMissingException.class));
    }
}
