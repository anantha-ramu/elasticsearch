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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.repositories.gcs.GcsStreamingUploadSettings.PART_SIZE_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class GcsStreamingUploadSettingsTests extends ESTestCase {

    private static Settings partSize(String value) {
        return Settings.builder().put(PART_SIZE_SETTING.getKey(), value).build();
    }

    public void testOffByDefault() {
        // unless the JVM property that switches it on for running the existing tests with streaming uploads is set
        if (System.getProperty(GcsStreamingUploadSettings.DEFAULT_PART_SIZE_PROPERTY) == null) {
            assertThat(PART_SIZE_SETTING.get(Settings.EMPTY), equalTo(ByteSizeValue.ZERO));
            assertFalse(new GcsStreamingUploadSettings(Settings.EMPTY, null, null).getConfig().isEnabled());
        }
    }

    public void testDefaults() {
        final var config = new GcsStreamingUploadSettings(partSize("100mb"), null, null).getConfig();
        assertTrue(config.isEnabled());
        assertThat(config.getPartSize(), equalTo(ByteSizeUnit.MB.toBytes(100)));
        assertTrue(config.isVerifyChecksum());
        assertThat(config.getMaxAttempts(), equalTo(10));
        assertThat(config.getMaxRetryTime(), equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(config.getInitialBackoff(), equalTo(TimeValue.timeValueMillis(500)));
        assertThat(config.getMaxBackoff(), equalTo(TimeValue.timeValueSeconds(30)));
        // off: over TLS the disconnect of the watchdog does not break a stalled write, see the setting
        assertThat(config.getWriteStallTimeout(), equalTo(TimeValue.ZERO));
    }

    public void testPartSizeMustBeAMultipleOf256kb() {
        for (String valid : new String[] { "0", "256kb", "512kb", "1mb", "100mb", "1gb" }) {
            assertThat(PART_SIZE_SETTING.get(partSize(valid)).getBytes() % (256 * 1024), equalTo(0L));
        }
        final long unit = 256 * 1024;
        for (String invalid : new String[] {
            "1b",
            "255kb",
            "257kb",
            (100 * 1024 * 1024 + 1) + "b",
            (unit * between(1, 100) + between(1, (int) unit - 1)) + "b" }) {
            final var e = expectThrows(IllegalArgumentException.class, () -> PART_SIZE_SETTING.get(partSize(invalid)));
            assertThat(invalid, e.getMessage(), containsString(PART_SIZE_SETTING.getKey()));
        }
    }

    public void testPartSizeMustFitTheLengthOfAnHttpBody() {
        // above this HttpURLConnection could not use fixed-length streaming and would fall back to chunked encoding
        final var e = expectThrows(IllegalArgumentException.class, () -> PART_SIZE_SETTING.get(partSize("2gb")));
        assertThat(e.getMessage(), containsString("must be 0 or between"));
        assertThat(
            PART_SIZE_SETTING.get(partSize(GcsStreamingUploadSettings.MAX_PART_SIZE_BYTES + "b")).getBytes(),
            equalTo(GcsStreamingUploadSettings.MAX_PART_SIZE_BYTES)
        );
        assertTrue(GcsStreamingUploadSettings.MAX_PART_SIZE_BYTES <= Integer.MAX_VALUE);
    }

    public void testDynamicUpdateAndValidation() {
        final var clusterSettings = new ClusterSettings(partSize("1mb"), Set.copyOf(GcsStreamingUploadSettings.getSettings()));
        final var settings = new GcsStreamingUploadSettings(partSize("1mb"), clusterSettings, null);
        assertThat(settings.getConfig().getPartSize(), equalTo(ByteSizeUnit.MB.toBytes(1)));

        clusterSettings.applySettings(
            Settings.builder()
                .put(PART_SIZE_SETTING.getKey(), "100mb")
                .put(GcsStreamingUploadSettings.MAX_ATTEMPTS_SETTING.getKey(), 3)
                .put(GcsStreamingUploadSettings.MAX_RETRY_TIME_SETTING.getKey(), "7s")
                .put(GcsStreamingUploadSettings.WRITE_STALL_TIMEOUT_SETTING.getKey(), "0")
                .put(GcsStreamingUploadSettings.VERIFY_CRC32C_SETTING.getKey(), false)
                .build()
        );
        final var updated = settings.getConfig();
        assertThat(updated.getPartSize(), equalTo(ByteSizeUnit.MB.toBytes(100)));
        assertThat(updated.getMaxAttempts(), equalTo(3));
        assertThat(updated.getMaxRetryTime(), equalTo(TimeValue.timeValueSeconds(7)));
        assertThat(updated.getWriteStallTimeout(), equalTo(TimeValue.ZERO));
        assertFalse(updated.isVerifyChecksum());

        expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(partSize("100mb1")));
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(partSize("300kb")));
        assertThat("a refused update changes nothing", settings.getConfig().getPartSize(), equalTo(ByteSizeUnit.MB.toBytes(100)));

        clusterSettings.applySettings(partSize("0"));
        assertFalse(settings.getConfig().isEnabled());
    }

    public void testClusterSettingsThatDoNotKnowTheSettingsFallBackToTheNodeSettings() {
        // as in tests that build a plain ClusterSettings
        final var settings = new GcsStreamingUploadSettings(
            partSize("1mb"),
            new ClusterSettings(Settings.EMPTY, Set.<Setting<?>>of()),
            null
        );
        assertThat(settings.getConfig().getPartSize(), equalTo(ByteSizeUnit.MB.toBytes(1)));
    }

    public void testPluginRegistersTheSettings() {
        assertThat(
            new GoogleCloudStoragePlugin(Settings.EMPTY).getSettings(),
            hasItems(GcsStreamingUploadSettings.getSettings().toArray(new Setting<?>[0]))
        );
    }
}
