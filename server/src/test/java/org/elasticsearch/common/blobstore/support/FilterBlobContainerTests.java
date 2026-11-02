/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests server-side copies between containers that are wrapped differently, as when the source comes from another store.
 */
public class FilterBlobContainerTests extends ESTestCase {

    public void testUnwrapReturnsThePluginContainer() throws IOException {
        final FsBlobContainer plain = fsContainer(createTempDir());
        assertThat(FilterBlobContainer.unwrap(plain), sameInstance(plain));
        assertThat(FilterBlobContainer.unwrap(wrap(plain, between(1, 3))), sameInstance(plain));
    }

    public void testCopyBlobWithMismatchedWrapping() throws IOException {
        final BlobContainer source = wrap(fsContainer(createTempDir()), between(0, 2));
        final BlobContainer destination = wrap(fsContainer(createTempDir()), between(0, 2));
        final byte[] contents = randomByteArrayOfLength(between(1, 4096));
        source.writeBlob(OperationPurpose.SNAPSHOT_DATA, "commit_blob", new BytesArray(contents), true);

        // the snapshot path always passes the innermost container as the source, whatever wraps either side
        destination.copyBlob(
            OperationPurpose.SNAPSHOT_DATA,
            FilterBlobContainer.unwrap(source),
            "commit_blob",
            "c__copy",
            contents.length,
            null
        );
        assertThat(readAll(destination, "c__copy"), equalTo(contents));

        // a wrapped destination also accepts a source that is still wrapped, at any depth
        if (destination instanceof FilterBlobContainer) {
            destination.copyBlob(OperationPurpose.SNAPSHOT_DATA, source, "commit_blob", "c__again", contents.length, null);
            assertThat(readAll(destination, "c__again"), equalTo(contents));
        }
    }

    private static FsBlobContainer fsContainer(Path path) throws IOException {
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false), BlobPath.EMPTY, path);
    }

    private static BlobContainer wrap(BlobContainer container, int times) {
        for (int i = 0; i < times; i++) {
            container = new Wrapper(container);
        }
        return container;
    }

    private static byte[] readAll(BlobContainer container, String blobName) throws IOException {
        try (InputStream in = container.readBlob(OperationPurpose.SNAPSHOT_DATA, blobName)) {
            return in.readAllBytes();
        }
    }

    private static final class Wrapper extends FilterBlobContainer {
        Wrapper(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new Wrapper(child);
        }
    }
}
