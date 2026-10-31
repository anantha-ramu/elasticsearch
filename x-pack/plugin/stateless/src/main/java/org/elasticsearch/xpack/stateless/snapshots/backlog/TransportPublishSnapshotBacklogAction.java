/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Receives an index node's latest snapshot backlog on the master and keeps it in the {@link SnapshotBacklogMetricsService}.
 * <p>
 * It always answers success: a report the master doesn't keep (from a node that left, an older one, or one that arrives while the
 * feature is off) needs nothing from the node, whose next report follows anyway.
 */
public class TransportPublishSnapshotBacklogAction extends TransportMasterNodeAction<PublishSnapshotBacklogRequest, ActionResponse.Empty> {

    public static final String NAME = "internal:admin/stateless/snapshot/publish_backlog";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);

    private final SnapshotBacklogMetricsService snapshotBacklogMetricsService;

    @Inject
    public TransportPublishSnapshotBacklogAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SnapshotBacklogMetricsService snapshotBacklogMetricsService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PublishSnapshotBacklogRequest::new,
            in -> ActionResponse.Empty.INSTANCE,
            // keeping a report is two cluster state reads and one map update; the request is already read on the transport thread
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.snapshotBacklogMetricsService = snapshotBacklogMetricsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PublishSnapshotBacklogRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            snapshotBacklogMetricsService.update(request);
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PublishSnapshotBacklogRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
