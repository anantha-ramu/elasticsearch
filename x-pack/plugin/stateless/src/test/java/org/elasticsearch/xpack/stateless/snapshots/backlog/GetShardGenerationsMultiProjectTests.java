/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.RestoreOnlyRepository;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Runs the master side of {@link TransportGetShardGenerationsAction} against a real {@link RepositoriesService} that holds a repository
 * of the same name in two projects. The stateless integration tests run a single project, so this is where the projects are told apart.
 */
public class GetShardGenerationsMultiProjectTests extends ESTestCase {

    private static final String REPOSITORY = "repo";
    private static final String INDEX = "index";
    private static final String CANNED_REPOSITORY_TYPE = "canned";

    /**
     * The repository data each project's repository answers with.
     */
    private final Map<ProjectId, RepositoryData> repositoryDataByProject = new ConcurrentHashMap<>();

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private TransportGetShardGenerationsAction action;

    @Before
    public void createAction() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        // the action registers its handler with the transport service, but the tests call the master operation directly, so nothing is
        // ever sent and the transport itself can be a mock
        final var transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Set.of()
        );
        final Map<String, Repository.Factory> internalTypes = Map.of(CANNED_REPOSITORY_TYPE, CannedRepository::new);
        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            Map.of(),
            internalTypes,
            threadPool,
            new NoOpNodeClient(threadPool),
            List.of()
        );
        repositoriesService.start();
        action = new TransportGetShardGenerationsAction(
            transportService,
            clusterService,
            threadPool,
            ActionFilters.EMPTY,
            repositoriesService
        );
    }

    @After
    public void closeAction() {
        repositoriesService.stop();
        repositoriesService.close();
        clusterService.close();
        terminate(threadPool);
    }

    private static RepositoryData repositoryData(long generation, RepositoryShardGeneration shardGeneration) {
        final var snapshot = new SnapshotId(randomIdentifier(), randomUUID());
        return new RepositoryData(
            randomUUID(),
            generation,
            Map.of(snapshot.getUUID(), snapshot),
            Map.of(),
            Map.of(shardGeneration.indexId(), List.of(snapshot)),
            ShardGenerations.builder().put(shardGeneration.indexId(), 0, shardGeneration.generation()).build(),
            IndexMetaDataGenerations.EMPTY,
            randomUUID()
        );
    }

    private void putRepository(ProjectId projectId, RepositoryData repositoryData) {
        repositoryDataByProject.put(projectId, repositoryData);
        repositoriesService.registerInternalRepository(projectId, REPOSITORY, CANNED_REPOSITORY_TYPE);
    }

    /**
     * Runs the master operation on the pool the action runs it on, as the master would.
     */
    private void runMasterOperation(ProjectId projectId, List<ShardId> shardIds, ActionListener<GetShardGenerationsResponse> listener) {
        final var request = new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, new ProjectRepo(projectId, REPOSITORY), shardIds);
        final var task = new Task(
            randomNonNegativeLong(),
            "transport",
            TransportGetShardGenerationsAction.NAME,
            "",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
            .execute(ActionRunnable.wrap(listener, l -> action.masterOperation(task, request, clusterService.state(), l)));
    }

    public void testEachProjectGetsItsOwnRepository() {
        final var project = randomProjectIdOrDefault();
        final var otherProject = randomValueOtherThan(project, ESTestCase::randomUniqueProjectId);
        // the same index name in both projects, with a different index id and shard generation in each project's repository
        final var shardGeneration = new RepositoryShardGeneration(new IndexId(INDEX, randomUUID()), new ShardGeneration(randomUUID()));
        final var otherShardGeneration = new RepositoryShardGeneration(new IndexId(INDEX, randomUUID()), new ShardGeneration(randomUUID()));
        final long repositoryGeneration = randomNonNegativeLong();
        final long otherRepositoryGeneration = randomValueOtherThan(repositoryGeneration, ESTestCase::randomNonNegativeLong);
        putRepository(project, repositoryData(repositoryGeneration, shardGeneration));
        putRepository(otherProject, repositoryData(otherRepositoryGeneration, otherShardGeneration));
        final var shardId = new ShardId(new Index(INDEX, randomUUID()), 0);

        assertThat(
            safeAwait((ActionListener<GetShardGenerationsResponse> l) -> runMasterOperation(project, List.of(shardId), l)),
            equalTo(new GetShardGenerationsResponse(repositoryGeneration, Map.of(shardId, shardGeneration)))
        );
        assertThat(
            safeAwait((ActionListener<GetShardGenerationsResponse> l) -> runMasterOperation(otherProject, List.of(shardId), l)),
            equalTo(new GetShardGenerationsResponse(otherRepositoryGeneration, Map.of(shardId, otherShardGeneration)))
        );
    }

    public void testRepositoryOfAnotherProjectIsNotUsed() {
        final var project = randomProjectIdOrDefault();
        final var otherProject = randomValueOtherThan(project, ESTestCase::randomUniqueProjectId);
        final var shardGeneration = new RepositoryShardGeneration(new IndexId(INDEX, randomUUID()), new ShardGeneration(randomUUID()));
        putRepository(project, repositoryData(randomNonNegativeLong(), shardGeneration));
        final var shardId = new ShardId(new Index(INDEX, randomUUID()), 0);

        final var e = safeAwaitFailure(GetShardGenerationsResponse.class, l -> runMasterOperation(otherProject, List.of(shardId), l));
        assertThat(e.getClass(), equalTo(RepositoryMissingException.class));
    }

    /**
     * A repository that answers with the repository data the test put for its project.
     */
    private class CannedRepository extends RestoreOnlyRepository {

        private final RepositoryMetadata metadata;

        CannedRepository(ProjectId projectId, RepositoryMetadata metadata) {
            super(projectId, INDEX);
            this.metadata = metadata;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
            listener.onResponse(repositoryDataByProject.get(getProjectId()));
        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
