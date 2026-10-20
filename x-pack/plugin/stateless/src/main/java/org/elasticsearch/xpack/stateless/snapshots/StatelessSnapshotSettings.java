/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME;

public class StatelessSnapshotSettings {

    public enum StatelessSnapshotEnabledStatus {
        /**
         * The stateless snapshot functionality is disabled
         */
        DISABLED(false),

        /**
         * The stateless snapshot reads indices data from the object store. But otherwise no changes.
         * The commit information is obtained directly from the local primary shard.
         */
        READ_FROM_OBJECT_STORE(false),

        /**
         * The stateless snapshot reads indices data from the object store.
         * The commit information is obtained from the primary shard via a transport request,
         * which allows the snapshot to be taken on a node that does not host the primary shard.
         */
        ENABLED(true);

        /**
         * Whether the enabled status supports relocation during snapshots.
         */
        private final boolean supportsRelocationDuringSnapshot;

        StatelessSnapshotEnabledStatus(boolean supportsRelocationDuringSnapshot) {
            this.supportsRelocationDuringSnapshot = supportsRelocationDuringSnapshot;
        }

        public boolean supportsRelocationDuringSnapshot() {
            return supportsRelocationDuringSnapshot;
        }

        public String description() {
            return this + " [supportsRelocationDuringSnapshot=" + supportsRelocationDuringSnapshot() + "]";
        }
    }

    public static final Setting<StatelessSnapshotEnabledStatus> STATELESS_SNAPSHOT_ENABLED_SETTING = Setting.enumSetting(
        StatelessSnapshotEnabledStatus.class,
        "stateless.snapshot.enabled",
        StatelessSnapshotEnabledStatus.DISABLED,
        new Setting.Validator<>() {
            @Override
            public void validate(StatelessSnapshotEnabledStatus value) {}

            @Override
            public void validate(StatelessSnapshotEnabledStatus value, Map<Setting<?>, Object> settings) {
                validateSettingsConsistency(value, (boolean) settings.get(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING));
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING = Setting.boolSetting(
        RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME,
        false,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                validateSettingsConsistency((StatelessSnapshotEnabledStatus) settings.get(STATELESS_SNAPSHOT_ENABLED_SETTING), value);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(STATELESS_SNAPSHOT_ENABLED_SETTING);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "stateless.snapshot.wait_for_active_primary_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Experimental: while a snapshot of a shard uploads a file that was read from the object store, read the file's next range from the
     * object store while the current one is uploaded, over the same connection, see {@link PrefetchingInputStream}. Takes effect for the
     * next file. Ignored when {@link #SOURCE_READ_PARALLEL_CONNECTIONS_SETTING} is greater than 1, which overlaps reads and writes as well.
     */
    public static final Setting<Boolean> SOURCE_READ_AHEAD_SETTING = Setting.boolSetting(
        "stateless.snapshot.read.read_ahead",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** The most connections that a file can be read over, which is also what the pool for the reads is sized for. */
    public static final int SOURCE_READ_MAX_PARALLEL_CONNECTIONS = 8;

    /**
     * Experimental: the number of connections over which a file is read from the object store. With more than 1, the file is split in
     * ranges of {@link #SOURCE_READ_RANGE_SIZE_SETTING}, up to this many of which are fetched at once, each with its own ranged GET, and
     * read in order. Takes effect for the next file. 1 reads the file as it is read without this setting.
     */
    public static final Setting<Integer> SOURCE_READ_PARALLEL_CONNECTIONS_SETTING = Setting.intSetting(
        "stateless.snapshot.read.parallel_connections",
        1,
        1,
        SOURCE_READ_MAX_PARALLEL_CONNECTIONS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Experimental: the size of a range that is read ahead or in parallel. Each file that is read ahead or in parallel holds up to one more
     * range than it has connections, in buffers of 256 KiB each, so there are no allocations that G1 handles as humongous.
     */
    public static final Setting<ByteSizeValue> SOURCE_READ_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.snapshot.read.range_size",
        ByteSizeValue.of(4, ByteSizeUnit.MB),
        ByteSizeValue.of(256, ByteSizeUnit.KB),
        ByteSizeValue.of(64, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Experimental: the most memory that all the files of the node that are read ahead or in parallel hold in buffers. A file that finds
     * the memory used up reads with fewer ranges in flight, or as a file is read without this setting. Defaults to a share of the heap, so
     * that small nodes stay safe.
     */
    public static final Setting<ByteSizeValue> SOURCE_READ_MAX_BUFFER_MEMORY_SETTING = Setting.memorySizeSetting(
        "stateless.snapshot.read.max_buffer_memory",
        "1.5%",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static void validateSettingsConsistency(
        StatelessSnapshotSettings.StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus,
        boolean relocationDuringSnapshotEnabled
    ) {
        if (statelessSnapshotEnabledStatus != StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
            && relocationDuringSnapshotEnabled) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Setting [%s] cannot be [true] unless setting [%s] is [%s]",
                    StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                    StatelessSnapshotSettings.StatelessSnapshotEnabledStatus.ENABLED
                )
            );
        }
    }
}
