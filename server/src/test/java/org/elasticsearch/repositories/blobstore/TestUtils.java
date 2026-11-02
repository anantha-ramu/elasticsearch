/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.nio.file.Path;
import java.util.function.UnaryOperator;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;

public class TestUtils {
    /** Create a {@link Repository} with a random name **/
    static Repository createRepository(ProjectId projectId, Path tempDir, NamedXContentRegistry xContentRegistry) {
        return createRepository(projectId, tempDir, xContentRegistry, null);
    }

    /**
     * Create a {@link Repository} with a random name. If {@code shardContainers} is not null, the repository passes every shard
     * container it uses through that function, so that a test can change what happens to the blobs in a shard's folder, for example
     * to inject a fault.
     */
    static Repository createRepository(
        ProjectId projectId,
        Path tempDir,
        NamedXContentRegistry xContentRegistry,
        @Nullable UnaryOperator<BlobContainer> shardContainers
    ) {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(projectId, repositoryMetadata);
        final Environment environment = createEnvironment(tempDir);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final FsRepository repository = shardContainers == null
            ? new FsRepository(
                projectId,
                repositoryMetadata,
                environment,
                xContentRegistry,
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                recoverySettings
            )
            : new FsRepository(
                projectId,
                repositoryMetadata,
                environment,
                xContentRegistry,
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                recoverySettings
            ) {
                @Override
                public BlobContainer shardContainer(IndexId indexId, int shardId) {
                    return shardContainers.apply(super.shardContainer(indexId, shardId));
                }
            };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    private static Environment createEnvironment(Path tempDir) {
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), tempDir.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
                .build()
        );
    }
}
