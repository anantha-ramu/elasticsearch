/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Sends this index node's latest snapshot backlog to the master ({@link TransportPublishSnapshotBacklogAction}).
 * <p>
 * Each report is the node's whole result, so a report that fails is not retried: the next evaluation sends a newer one 30 seconds
 * later. For the same reason, at most one report is in flight: if the previous one hasn't completed yet, a new one is skipped. A report
 * waits at most one evaluation interval for a master.
 * <p>
 * Nothing is sent while the cluster has a node too old to know the action, since the master can be any node.
 */
final class SnapshotBacklogPublisher {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogPublisher.class);

    static final TransportVersion PUBLISH_ACTION = TransportVersion.fromName("snapshot_backlog_publish");

    /**
     * Sends one report to the master.
     */
    @FunctionalInterface
    interface Sender {
        void send(PublishSnapshotBacklogRequest request, ActionListener<ActionResponse.Empty> listener);
    }

    private final Sender sender;
    private final LongSupplier relativeTimeMillis;
    private final AtomicLong seqNo = new AtomicLong();
    private final AtomicBoolean inFlight = new AtomicBoolean();

    // guarded by this
    private int failuresSinceWarning;
    // guarded by this
    private long nextWarningMillis = Long.MIN_VALUE;

    SnapshotBacklogPublisher(Sender sender, LongSupplier relativeTimeMillis) {
        this.sender = sender;
        this.relativeTimeMillis = relativeTimeMillis;
    }

    /**
     * Sends the reports in the system context: the action is internal, and serves the node, not any user's request.
     */
    static Sender systemContextSender(Client client) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        return (request, listener) -> {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                client.execute(TransportPublishSnapshotBacklogAction.TYPE, request, listener);
            }
        };
    }

    static boolean isSupported(ClusterState state) {
        return state.getMinTransportVersion().supports(PUBLISH_ACTION);
    }

    /**
     * Sends the node's latest backlog to the master, unless the cluster is too old or the previous report is still in flight.
     *
     * @param state  the cluster state the evaluation used
     * @param shards per repository, per started primary on this node, the backlog
     */
    void publish(ClusterState state, Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards) {
        if (isSupported(state) == false) {
            return;
        }
        if (inFlight.compareAndSet(false, true) == false) {
            logger.debug("not sending the snapshot backlog to the master, the previous report is still in flight");
            return;
        }
        final DiscoveryNode localNode = state.nodes().getLocalNode();
        final PublishSnapshotBacklogRequest request = new PublishSnapshotBacklogRequest(
            SnapshotBacklogTracker.EVALUATION_INTERVAL,
            localNode.getId(),
            localNode.getEphemeralId(),
            seqNo.incrementAndGet(),
            state.version(),
            shards
        );
        ActionListener.run(
            ActionListener.runAfter(ActionListener.<ActionResponse.Empty>wrap(ignored -> {}, this::onFailure), () -> inFlight.set(false)),
            listener -> sender.send(request, listener)
        );
    }

    private void onFailure(Exception e) {
        logger.debug("failed to send the snapshot backlog to the master", e);
        final long now = relativeTimeMillis.getAsLong();
        synchronized (this) {
            failuresSinceWarning += 1;
            if (now >= nextWarningMillis) {
                logger.warn(
                    "failed to send the snapshot backlog to the master {} time(s) since the last warning; the latest failure was [{}]",
                    failuresSinceWarning,
                    e.getMessage()
                );
                failuresSinceWarning = 0;
                nextWarningMillis = now + NextShardGenerations.WARNING_INTERVAL.millis();
            }
        }
    }
}
