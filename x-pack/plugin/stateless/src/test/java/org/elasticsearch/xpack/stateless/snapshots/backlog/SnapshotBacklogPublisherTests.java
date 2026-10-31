/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.security.authz.privilege.SystemPrivilege;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SnapshotBacklogPublisherTests extends ESTestCase {

    private static final DiscoveryNode LOCAL = DiscoveryNodeUtils.builder("local").ephemeralId("local-run").build();
    private static final Map<ProjectRepo, Map<ShardId, ShardBacklog>> SHARDS = Map.of(
        new ProjectRepo(ProjectId.DEFAULT, "repo"),
        Map.of(new ShardId(new Index("index", "index-uuid"), 0), new ShardBacklog("copy-0", new ShardGeneration("generation"), 100))
    );

    private record Sent(PublishSnapshotBacklogRequest request, ActionListener<ActionResponse.Empty> listener) {}

    private final List<Sent> sent = new ArrayList<>();
    private final SnapshotBacklogPublisher publisher = new SnapshotBacklogPublisher(
        (request, listener) -> sent.add(new Sent(request, listener)),
        () -> 0L
    );

    public void testReportCarriesTheNode() {
        final ClusterState state = state(TransportVersion.current());
        publisher.publish(state, SHARDS);

        assertThat(sent, hasSize(1));
        final PublishSnapshotBacklogRequest request = sent.get(0).request();
        assertThat(request.nodeId(), equalTo("local"));
        assertThat(request.nodeEphemeralId(), equalTo("local-run"));
        assertThat(request.seqNo(), equalTo(1L));
        assertThat(request.shards(), equalTo(SHARDS));
        // a report waits at most one evaluation interval for a master
        assertThat(request.masterNodeTimeout(), equalTo(SnapshotBacklogTracker.EVALUATION_INTERVAL));
    }

    public void testNoReportWhileClusterIsTooOld() {
        final TransportVersion older = TransportVersionUtils.getPreviousVersion(SnapshotBacklogPublisher.PUBLISH_ACTION);
        publisher.publish(state(older), SHARDS);
        assertThat(sent, empty());

        // Once every node is upgraded, the report is sent.
        publisher.publish(state(TransportVersion.current()), SHARDS);
        assertThat(sent, hasSize(1));
    }

    public void testOneReportAtATime() {
        publisher.publish(state(TransportVersion.current()), SHARDS);
        publisher.publish(state(TransportVersion.current()), SHARDS);
        // the second was skipped, the first is still in flight
        assertThat(sent, hasSize(1));

        sent.get(0).listener().onResponse(ActionResponse.Empty.INSTANCE);
        publisher.publish(state(TransportVersion.current()), SHARDS);
        assertThat(sent, hasSize(2));
        assertThat(sent.get(1).request().seqNo(), equalTo(2L));
    }

    public void testFailureIsNotRetried() {
        publisher.publish(state(TransportVersion.current()), SHARDS);
        sent.get(0).listener().onFailure(new IllegalStateException("no master"));
        assertThat(sent, hasSize(1));

        // the next evaluation's report goes out as usual
        publisher.publish(state(TransportVersion.current()), SHARDS);
        assertThat(sent, hasSize(2));
        assertThat(sent.get(1).request().seqNo(), equalTo(2L));
    }

    public void testSenderThatThrowsFreesTheSlot() {
        final AtomicBoolean fail = new AtomicBoolean(true);
        final SnapshotBacklogPublisher throwing = new SnapshotBacklogPublisher((request, listener) -> {
            if (fail.get()) {
                throw new IllegalStateException("cannot send");
            }
            sent.add(new Sent(request, listener));
        }, () -> 0L);
        throwing.publish(state(TransportVersion.current()), SHARDS);
        fail.set(false);
        throwing.publish(state(TransportVersion.current()), SHARDS);
        assertThat(sent, hasSize(1));
    }

    public void testReportIsSentInSystemContext() {
        final AtomicBoolean systemContext = new AtomicBoolean();
        final List<String> headers = new ArrayList<>();
        final var threadPool = new DeterministicTaskQueue().getThreadPool();
        final var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, equalTo(TransportPublishSnapshotBacklogAction.TYPE));
                systemContext.set(threadPool.getThreadContext().isSystemContext());
                headers.addAll(threadPool.getThreadContext().getHeaders().keySet());
            }
        };
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller-header", "value");
            SnapshotBacklogPublisher.systemContextSender(client)
                .send(new PublishSnapshotBacklogRequest(TEST_REQUEST_TIMEOUT, "local", "local-run", 1, SHARDS), ActionListener.noop());
            // The caller's context is back afterwards.
            assertFalse(threadContext.isSystemContext());
            assertThat(threadContext.getHeader("caller-header"), equalTo("value"));
        }

        assertTrue(systemContext.get());
        assertThat(headers, empty());
        // On a secured cluster the system user runs the action, and may run every internal action.
        assertTrue(SystemPrivilege.INSTANCE.predicate().test(TransportPublishSnapshotBacklogAction.NAME));
    }

    private static ClusterState state(TransportVersion minTransportVersion) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(LOCAL).localNodeId(LOCAL.getId()))
            .putCompatibilityVersions(LOCAL.getId(), new CompatibilityVersions(minTransportVersion, Map.of()))
            .build();
    }
}
