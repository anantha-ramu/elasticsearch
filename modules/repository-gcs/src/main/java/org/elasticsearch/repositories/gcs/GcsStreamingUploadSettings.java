/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Dynamic cluster settings for the streaming snapshot upload, see {@link GcsStreamingUpload}. The values are read at write time, so a
 * change applies to the next file written and never to an upload in flight.
 */
final class GcsStreamingUploadSettings {

    /**
     * GCS only accepts non-final chunks of a resumable upload that are a multiple of this size.
     */
    static final long PART_ALIGNMENT_BYTES = ByteSizeUnit.KB.toBytes(256);

    /**
     * The request body length must fit in an int for HttpURLConnection to use fixed-length streaming mode (otherwise the HTTP client
     * falls back to chunked encoding), so this is the largest multiple of {@link #PART_ALIGNMENT_BYTES} below 2GiB.
     */
    static final long MAX_PART_SIZE_BYTES = ByteSizeUnit.GB.toBytes(2) - PART_ALIGNMENT_BYTES;

    /**
     * JVM property that overrides the default part size. Meant for running the existing tests with streaming uploads switched on.
     */
    static final String DEFAULT_PART_SIZE_PROPERTY = "es.repository_gcs.streaming_upload.part_size";

    private static ByteSizeValue defaultPartSize() {
        final String value = System.getProperty(DEFAULT_PART_SIZE_PROPERTY);
        return value == null ? ByteSizeValue.ZERO : parsePartSize(value, DEFAULT_PART_SIZE_PROPERTY);
    }

    static ByteSizeValue parsePartSize(String value, String key) {
        final ByteSizeValue size = ByteSizeValue.parseBytesSizeValue(value, key);
        final long bytes = size.getBytes();
        if (bytes == 0) {
            return size;
        }
        if (bytes < PART_ALIGNMENT_BYTES || bytes > MAX_PART_SIZE_BYTES) {
            throw new IllegalArgumentException(
                "setting [" + key + "] must be 0 or between [256kb] and [" + MAX_PART_SIZE_BYTES + "b] but was [" + value + "]"
            );
        }
        if (bytes % PART_ALIGNMENT_BYTES != 0) {
            throw new IllegalArgumentException("setting [" + key + "] must be 0 or a multiple of [256kb] but was [" + value + "]");
        }
        return size;
    }

    /**
     * Size of each part of a streamed upload. 0 (the default) switches streaming off and snapshot data is uploaded through the GCS SDK as
     * before. Must be a multiple of 256kb.
     */
    static final Setting<ByteSizeValue> PART_SIZE_SETTING = new Setting<>(
        "gcs.streaming_upload.part_size",
        s -> defaultPartSize().getStringRep(),
        s -> parsePartSize(s, "gcs.streaming_upload.part_size"),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Number of consecutive failed requests, without any progress in the committed offset, after which the upload is abandoned.
     */
    static final Setting<Integer> MAX_ATTEMPTS_SETTING = Setting.intSetting(
        "gcs.streaming_upload.max_attempts",
        10,
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Time after the first failure of a streak of failures, without any progress in the committed offset, after which the upload is
     * abandoned.
     */
    static final Setting<TimeValue> MAX_RETRY_TIME_SETTING = Setting.timeSetting(
        "gcs.streaming_upload.max_retry_time",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMillis(1),
        Property.Dynamic,
        Property.NodeScope
    );

    static final Setting<TimeValue> INITIAL_BACKOFF_SETTING = Setting.timeSetting(
        "gcs.streaming_upload.initial_backoff",
        TimeValue.timeValueMillis(500),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.NodeScope
    );

    static final Setting<TimeValue> MAX_BACKOFF_SETTING = Setting.timeSetting(
        "gcs.streaming_upload.max_backoff",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * HttpURLConnection has no write timeout. A part whose socket write makes no progress for this long is disconnected and retried.
     * 0 disables the watchdog.
     */
    static final Setting<TimeValue> WRITE_STALL_TIMEOUT_SETTING = Setting.timeSetting(
        "gcs.streaming_upload.write_stall_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Whether to verify a CRC32C of the whole blob, as the SDK does. The checksum has to be in the headers of the last request, before its
     * body, so the last 256kb or less of the blob is sent as a request of its own, once it is known. That is one request more per blob.
     */
    static final Setting<Boolean> VERIFY_CRC32C_SETTING = Setting.boolSetting(
        "gcs.streaming_upload.verify_crc32c",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    static List<Setting<?>> getSettings() {
        return List.of(
            PART_SIZE_SETTING,
            VERIFY_CRC32C_SETTING,
            MAX_ATTEMPTS_SETTING,
            MAX_RETRY_TIME_SETTING,
            INITIAL_BACKOFF_SETTING,
            MAX_BACKOFF_SETTING,
            WRITE_STALL_TIMEOUT_SETTING
        );
    }

    /**
     * Immutable snapshot of the settings, taken once per file written.
     */
    static final class Config {
        static final Config DISABLED = new Config(0, true, 1, TimeValue.ZERO, TimeValue.ZERO, TimeValue.ZERO, TimeValue.ZERO);

        private final long partSize;
        private final boolean verifyChecksum;
        private final int maxAttempts;
        private final TimeValue maxRetryTime;
        private final TimeValue initialBackoff;
        private final TimeValue maxBackoff;
        private final TimeValue writeStallTimeout;

        Config(
            long partSize,
            boolean verifyChecksum,
            int maxAttempts,
            TimeValue maxRetryTime,
            TimeValue initialBackoff,
            TimeValue maxBackoff,
            TimeValue writeStallTimeout
        ) {
            assert partSize == 0 || (partSize % PART_ALIGNMENT_BYTES == 0 && partSize <= MAX_PART_SIZE_BYTES) : partSize;
            this.partSize = partSize;
            this.verifyChecksum = verifyChecksum;
            this.maxAttempts = maxAttempts;
            this.maxRetryTime = maxRetryTime;
            this.initialBackoff = initialBackoff;
            this.maxBackoff = maxBackoff;
            this.writeStallTimeout = writeStallTimeout;
        }

        boolean isEnabled() {
            return partSize > 0;
        }

        long getPartSize() {
            return partSize;
        }

        boolean isVerifyChecksum() {
            return verifyChecksum;
        }

        int getMaxAttempts() {
            return maxAttempts;
        }

        TimeValue getMaxRetryTime() {
            return maxRetryTime;
        }

        TimeValue getInitialBackoff() {
            return initialBackoff;
        }

        TimeValue getMaxBackoff() {
            return maxBackoff;
        }

        TimeValue getWriteStallTimeout() {
            return writeStallTimeout;
        }
    }

    private volatile long partSize;
    private volatile boolean verifyChecksum;
    private volatile int maxAttempts;
    private volatile TimeValue maxRetryTime;
    private volatile TimeValue initialBackoff;
    private volatile TimeValue maxBackoff;
    private volatile TimeValue writeStallTimeout;
    private final ThreadPool threadPool;

    /**
     * @param clusterSettings the cluster settings to track for dynamic updates. If it does not know about the settings (only expected in
     *                        tests that build a plain {@link ClusterSettings}) the values of the node settings are used and never change.
     */
    GcsStreamingUploadSettings(Settings nodeSettings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.partSize = PART_SIZE_SETTING.get(nodeSettings).getBytes();
        this.verifyChecksum = VERIFY_CRC32C_SETTING.get(nodeSettings);
        this.maxAttempts = MAX_ATTEMPTS_SETTING.get(nodeSettings);
        this.maxRetryTime = MAX_RETRY_TIME_SETTING.get(nodeSettings);
        this.initialBackoff = INITIAL_BACKOFF_SETTING.get(nodeSettings);
        this.maxBackoff = MAX_BACKOFF_SETTING.get(nodeSettings);
        this.writeStallTimeout = WRITE_STALL_TIMEOUT_SETTING.get(nodeSettings);
        if (clusterSettings != null && getSettings().stream().allMatch(s -> clusterSettings.get(s.getKey()) != null)) {
            clusterSettings.initializeAndWatch(PART_SIZE_SETTING, v -> this.partSize = v.getBytes());
            clusterSettings.initializeAndWatch(VERIFY_CRC32C_SETTING, v -> this.verifyChecksum = v);
            clusterSettings.initializeAndWatch(MAX_ATTEMPTS_SETTING, v -> this.maxAttempts = v);
            clusterSettings.initializeAndWatch(MAX_RETRY_TIME_SETTING, v -> this.maxRetryTime = v);
            clusterSettings.initializeAndWatch(INITIAL_BACKOFF_SETTING, v -> this.initialBackoff = v);
            clusterSettings.initializeAndWatch(MAX_BACKOFF_SETTING, v -> this.maxBackoff = v);
            clusterSettings.initializeAndWatch(WRITE_STALL_TIMEOUT_SETTING, v -> this.writeStallTimeout = v);
        }
    }

    /**
     * @return the current values; each call returns a consistent snapshot that stays fixed for the file being written
     */
    Config getConfig() {
        return new Config(partSize, verifyChecksum, maxAttempts, maxRetryTime, initialBackoff, maxBackoff, writeStallTimeout);
    }

    /**
     * The thread pool whose scheduler runs the write stall watchdogs of the uploads.
     */
    ThreadPool getThreadPool() {
        return threadPool;
    }
}
