/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * The cluster feature that tells a writer whether every node in the cluster can read copied files in snapshots.
 */
public class SnapshotCopiedBlobFeatures implements FeatureSpecification {

    /**
     * A copied blob is a copy of a whole commit blob, made by the object store itself. A copied file is a file that is stored as a byte
     * range of a copied blob. The shard's snapshot metadata records a copied file with three fields: {@code blob}, the name of the copied
     * blob; {@code offset}, the position in the copied blob where the file starts; and {@code source}, the path of the commit blob that
     * was copied. See {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo#isCopied()}.
     * <p>
     * A node with this feature can read, delete, clone and restore snapshots that hold copied files.
     * <p>
     * A node without this feature cannot parse the metadata that records a copied file, because its parser rejects fields it does not
     * know. That node then fails to take, clone or delete a snapshot of the shard, and to restore a snapshot that holds a copied file. So
     * code that records a copied file in the shard's snapshot metadata must first call
     * {@link org.elasticsearch.features.FeatureService#clusterHasFeature}. It may record the file only if every node in the cluster has
     * this feature.
     * <p>
     * Never set {@code assumedAfterNextCompatibilityBoundary} to true for this feature. With that flag, {@code clusterHasFeature} also
     * counts a node that does not publish the feature, if the build of that node reports
     * {@link org.elasticsearch.env.BuildVersion#canRemoveAssumedFeatures()} as true. A build that always reports true, such as the
     * serverless build, would make the check pass while nodes that cannot read copied files are still in the cluster.
     */
    public static final NodeFeature COPIED_BLOB_RANGES = new NodeFeature("snapshots.copied_blob_ranges");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(COPIED_BLOB_RANGES);
    }
}
