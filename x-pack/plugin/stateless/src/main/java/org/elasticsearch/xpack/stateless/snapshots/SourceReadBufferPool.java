/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.core.Nullable;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Experimental. The buffers that the files of a node that are read ahead or in parallel hold, see {@link PrefetchingInputStream}. A buffer
 * is a number of pages of {@link #PAGE_SIZE_BYTES}, which is small enough for G1 to never handle them as humongous allocations, whatever
 * the region size. The pages are kept for reuse, so that reading does not allocate once the pool is warm.
 * <p>
 * The pool has a cap on the pages that are in use at any one time, across all the files of the node. {@link #tryAcquire} never waits: it
 * returns {@code null} when the cap would be exceeded, and the caller reads with less in flight.
 */
final class SourceReadBufferPool {

    static final int PAGE_SIZE_BYTES = 256 * 1024;

    private final int pageSize;
    private final ConcurrentLinkedQueue<byte[]> free = new ConcurrentLinkedQueue<>();
    private final AtomicInteger freeCount = new AtomicInteger();
    private final AtomicLong pagesInUse = new AtomicLong();
    private volatile long capPages;
    private volatile boolean retainPages = true;

    SourceReadBufferPool(long capBytes) {
        this(PAGE_SIZE_BYTES, capBytes);
    }

    SourceReadBufferPool(int pageSize, long capBytes) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive, but was [" + pageSize + "]");
        }
        this.pageSize = pageSize;
        setCapBytes(capBytes);
    }

    int getPageSize() {
        return pageSize;
    }

    void setCapBytes(long capBytes) {
        this.capPages = Math.max(0, capBytes) / pageSize;
        trimFree();
    }

    /**
     * Whether released pages are kept for reuse. A node that does not read ahead keeps none, so that the pages of a read that was still
     * going when the settings were switched off are not held on to after it ends.
     */
    void setRetainPages(boolean retainPages) {
        this.retainPages = retainPages;
        if (retainPages == false) {
            clear();
        }
    }

    long getCapBytes() {
        return capPages * pageSize;
    }

    /**
     * @return the bytes of the buffers that are in use, which is 0 when every one was released
     */
    long getBytesInUse() {
        return pagesInUse.get() * pageSize;
    }

    long getFreePages() {
        return freeCount.get();
    }

    /**
     * Drops the pages that are kept for reuse, so that a node that no longer reads ahead holds no memory for it.
     */
    void clear() {
        while (free.poll() != null) {
            freeCount.decrementAndGet();
        }
    }

    /**
     * @param bytes the size of the buffer, which is rounded up to whole pages
     * @return a buffer, or {@code null} if that would put the pages in use over the cap
     */
    @Nullable
    Buffer tryAcquire(long bytes) {
        final int pages = pagesFor(bytes);
        while (true) {
            final long inUse = pagesInUse.get();
            if (inUse + pages > capPages) {
                return null;
            }
            if (pagesInUse.compareAndSet(inUse, inUse + pages)) {
                break;
            }
        }
        final byte[][] buffers = new byte[pages][];
        for (int i = 0; i < pages; i++) {
            byte[] page = free.poll();
            if (page == null) {
                page = new byte[pageSize];
            } else {
                freeCount.decrementAndGet();
            }
            buffers[i] = page;
        }
        return new Buffer(buffers);
    }

    int pagesFor(long bytes) {
        return Math.toIntExact((bytes + pageSize - 1) / pageSize);
    }

    private void trimFree() {
        // the pages kept for reuse never outnumber the cap, so that the pool does not hold more than it may lend
        while (freeCount.get() > capPages) {
            if (free.poll() == null) {
                break;
            }
            freeCount.decrementAndGet();
        }
    }

    /**
     * The pages of a range. {@link #release()} returns them, once only, however often it is called.
     */
    final class Buffer {
        private final byte[][] pages;
        private final AtomicBoolean released = new AtomicBoolean();

        private Buffer(byte[][] pages) {
            this.pages = pages;
        }

        /**
         * @return the page that holds the bytes from {@code pageIndex * pageSize}
         */
        byte[] page(int pageIndex) {
            return pages[pageIndex];
        }

        boolean isReleased() {
            return released.get();
        }

        void release() {
            if (released.compareAndSet(false, true)) {
                for (int i = 0; i < pages.length; i++) {
                    if (retainPages && freeCount.get() < capPages) {
                        freeCount.incrementAndGet();
                        free.add(pages[i]);
                    }
                    pages[i] = null;
                }
                pagesInUse.addAndGet(-pages.length);
            }
        }
    }
}
