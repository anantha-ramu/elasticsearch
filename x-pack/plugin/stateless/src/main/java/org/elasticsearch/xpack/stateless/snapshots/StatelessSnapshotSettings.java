/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
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

    /**
     * Whether a shard snapshot copies its new files into the snapshot repository instead of uploading them. A new file is a file that no
     * earlier snapshot of the shard has stored. Stateless Elasticsearch stores the files of a shard in the object store, in commit blobs.
     * A commit blob holds the new files of one or more Lucene commits. With this setting on, the object store itself copies a whole commit
     * blob into the snapshot repository, so the node reads and sends no file data. The copy is called a copied blob, and the shard's
     * snapshot metadata records each new file as a byte range of the copied blob. With this setting off, the node reads each new file and
     * uploads it.
     * <p>
     * Nothing reads this setting yet. The code that copies ships in a later release, and this release registers the setting before that
     * code ships. Elasticsearch rejects a setting that it does not know. A node whose configuration contains an unknown setting refuses to
     * start. A settings update that contains an unknown setting is rejected as a whole, and none of its other settings are applied.
     * During an upgrade, some nodes still run a release without the setting. So every node must know the setting before anyone sets it
     * to {@code true}.
     * <p>
     * The code that ships later is meant to copy only when this setting is on and every node in the cluster has the cluster feature
     * {@link org.elasticsearch.snapshots.SnapshotCopiedBlobFeatures#COPIED_BLOB_RANGES}, which says that the node can read copied files.
     * It also needs {@link #STATELESS_SNAPSHOT_ENABLED_SETTING} to be {@code read_from_object_store} or {@code enabled}, because only
     * then do shard snapshots read from the object store.
     * <p>
     * The setting is dynamic and operator-only. It can be changed while nodes are running. On a cluster that has operator privileges
     * turned on, only operator users can change it.
     */
    public static final Setting<Boolean> SERVER_SIDE_COPY_ENABLED_SETTING = Setting.boolSetting(
        "stateless.snapshot.server_side_copy.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    public static final Setting<TimeValue> STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "stateless.snapshot.wait_for_active_primary_timeout",
        TimeValue.timeValueMinutes(5),
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
