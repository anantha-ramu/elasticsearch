/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reads a shard's backup metadata from a repository on this node, straight into a baseline ({@link ShardBaselineFormat}).
 * <p>
 * It reads the blob of exactly the generation it is given, and fails if that blob is missing. It never lists the shard's folder and
 * never falls back to another blob, as {@link BlobStoreRepository#getBlobStoreIndexShardSnapshots} can: the cache takes the shard's
 * commit before reading, which is only right for a generation published after its blob was written in full (see
 * {@link ShardBaselineCache.MetadataReader}).
 */
final class BlobStoreMetadataReader implements ShardBaselineCache.MetadataReader {

    static final String READS_METRIC = "es.stateless.snapshot_backlog.metadata_reads.total";
    static final String READ_FAILURES_METRIC = "es.stateless.snapshot_backlog.metadata_read_failures.total";

    private final RepositoriesService repositoriesService;
    private final LongCounter reads;
    private final LongCounter readFailures;

    BlobStoreMetadataReader(RepositoriesService repositoriesService, MeterRegistry meterRegistry) {
        this.repositoriesService = repositoriesService;
        this.reads = meterRegistry.registerLongCounter(
            READS_METRIC,
            "Reads of shard backup metadata to measure the snapshot backlog, including failed ones",
            "unit"
        );
        this.readFailures = meterRegistry.registerLongCounter(
            READ_FAILURES_METRIC,
            "Failed reads of shard backup metadata to measure the snapshot backlog",
            "unit"
        );
    }

    @Override
    public ShardBaseline read(
        ProjectRepo projectRepo,
        IndexId indexId,
        int shardId,
        ShardGeneration generation,
        List<LuceneFile> currentFiles
    ) throws IOException {
        final Map<String, Object> attributes = Map.of("repo_name", projectRepo.name());
        reads.incrementBy(1, attributes);
        try {
            final Repository repository = repositoriesService.repository(projectRepo.projectId(), projectRepo.name());
            if (repository instanceof BlobStoreRepository blobStoreRepository) {
                return ShardBaselineFormat.forFiles(currentFiles)
                    .read(
                        projectRepo,
                        blobStoreRepository.shardContainer(indexId, shardId),
                        generation.getGenerationUUID(),
                        NamedXContentRegistry.EMPTY
                    );
            }
            throw new RepositoryException(projectRepo.name(), "is not a blob store repository");
        } catch (Exception e) {
            readFailures.incrementBy(1, attributes);
            throw e;
        }
    }
}
