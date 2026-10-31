/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * One index node's latest snapshot backlog, sent to the master after each evaluation. See {@link TransportPublishSnapshotBacklogAction}.
 * <p>
 * A report is complete for the node: it lists every started primary on the node, for every repository the node measures for that
 * shard's project, either measured or not measured. So the master can tell a measured 0 from a shard the node didn't report, and a
 * newer report simply replaces the older report.
 */
public class PublishSnapshotBacklogRequest extends MasterNodeRequest<PublishSnapshotBacklogRequest> {

    /**
     * The backlog of one shard copy in one repository.
     *
     * @param allocationId the allocation id of the shard copy that was measured, so the master never uses a figure of an earlier copy
     * @param generation   the generation of the shard's backup metadata the figure was measured against, or {@code null} if the shard
     *                     was not measured
     * @param bytes        the bytes the next snapshot of the shard would upload; 0 if not measured
     */
    public record ShardBacklog(String allocationId, @Nullable ShardGeneration generation, long bytes) implements Writeable {

        public ShardBacklog {
            Objects.requireNonNull(allocationId);
            assert generation != null || bytes == 0 : "a shard that was not measured has no bytes";
            assert bytes >= 0 : bytes;
        }

        static ShardBacklog notMeasured(String allocationId) {
            return new ShardBacklog(allocationId, null, 0);
        }

        boolean isMeasured() {
            return generation != null;
        }

        static ShardBacklog readFrom(StreamInput in) throws IOException {
            final String allocationId = in.readString();
            final ShardGeneration generation = in.readOptionalWriteable(ShardGeneration::new);
            // the bytes are only on the wire for a measured shard
            return new ShardBacklog(allocationId, generation, generation == null ? 0 : in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(allocationId);
            out.writeOptionalWriteable(generation);
            if (generation != null) {
                out.writeVLong(bytes);
            }
        }
    }

    private final String nodeId;
    private final String nodeEphemeralId;
    private final long seqNo;
    private final Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards;

    /**
     * @param nodeId          the id of the node that sends the report
     * @param nodeEphemeralId the ephemeral id of the node that sends the report, which changes each time the node restarts
     * @param seqNo           goes up by one with each report of this node process
     * @param shards          per repository, per shard, the backlog
     */
    public PublishSnapshotBacklogRequest(
        TimeValue masterNodeTimeout,
        String nodeId,
        String nodeEphemeralId,
        long seqNo,
        Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards
    ) {
        super(masterNodeTimeout);
        this.nodeId = Objects.requireNonNull(nodeId);
        this.nodeEphemeralId = Objects.requireNonNull(nodeEphemeralId);
        this.seqNo = seqNo;
        this.shards = Map.copyOf(shards);
    }

    public PublishSnapshotBacklogRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.nodeEphemeralId = in.readString();
        this.seqNo = in.readVLong();
        this.shards = in.readImmutableMap(ProjectRepo::new, i -> i.readImmutableMap(ShardId::new, ShardBacklog::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeString(nodeEphemeralId);
        out.writeVLong(seqNo);
        out.writeMap(shards, StreamOutput::writeWriteable, (o, byShard) -> o.writeMap(byShard));
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String nodeId() {
        return nodeId;
    }

    public String nodeEphemeralId() {
        return nodeEphemeralId;
    }

    public long seqNo() {
        return seqNo;
    }

    public Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards() {
        return shards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishSnapshotBacklogRequest that = (PublishSnapshotBacklogRequest) o;
        return seqNo == that.seqNo
            && nodeId.equals(that.nodeId)
            && nodeEphemeralId.equals(that.nodeEphemeralId)
            && shards.equals(that.shards)
            && Objects.equals(masterNodeTimeout(), that.masterNodeTimeout());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeEphemeralId, seqNo, shards, masterNodeTimeout());
    }

    @Override
    public String toString() {
        return "PublishSnapshotBacklogRequest{node=" + nodeId + ", seqNo=" + seqNo + ", repositories=" + shards.size() + '}';
    }
}
