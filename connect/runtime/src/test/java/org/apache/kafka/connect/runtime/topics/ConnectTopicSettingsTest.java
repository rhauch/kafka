/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.topics;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.storage.TopicSettings;
import org.apache.kafka.connect.storage.TopicSettings.CleanupPolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ConnectTopicSettingsTest {

    private final String TOPIC_NAME = "my-topic";

    private ConnectTopicSettings settings;

    @Before
    public void setUp() {
        settings = new ConnectTopicSettings(TOPIC_NAME);
    }

    @Test
    public void testParseCleanupPolicy() {
        String[] compacts = {TopicConfig.CLEANUP_POLICY_COMPACT, "compact", "  compact ", "ComPAcT", " ComPAcT  ", "COMPACT"};
        String[] deletes = {TopicConfig.CLEANUP_POLICY_DELETE, "delete", "  delete ", "DeLeTE", " DeLeTE  ", "DELETE"};
        for (String compact : compacts) {
            assertEquals(CleanupPolicy.COMPACT, ConnectTopicSettings.parseCleanupPolicy(compact));
        }
        for (String delete : deletes) {
            assertEquals(CleanupPolicy.DELETE, ConnectTopicSettings.parseCleanupPolicy(delete));
        }
        for (String compact : compacts) {
            for (String delete : deletes) {
                assertEquals(CleanupPolicy.COMPACT_AND_DELETE, ConnectTopicSettings.parseCleanupPolicy(compact + "," + delete));
                assertEquals(CleanupPolicy.COMPACT_AND_DELETE, ConnectTopicSettings.parseCleanupPolicy(delete + "," + compact + ","));
            }
        }
    }

    @Test
    public void testParseCleanupPolicyWithNull() {
        assertNull(ConnectTopicSettings.parseCleanupPolicy(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullName() {
        settings = new ConnectTopicSettings(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyName() {
        settings = new ConnectTopicSettings("  ");
    }

    @Test
    public void testName() {
        assertSame(TOPIC_NAME, settings.name());
    }

    @Test
    public void testPartitions() {
        assertEquals(ConnectTopicSettings.DEFAULT_NUM_PARTITIONS, settings.partitions());
        settings.partitions(44);
        assertEquals(44, settings.partitions());
    }

    @Test
    public void testReplicationFactor() {
        assertEquals(ConnectTopicSettings.DEFAULT_REPLICATION_FACTOR, settings.replicationFactor());
        settings.replicationFactor((short)44);
        assertEquals((short)44, settings.replicationFactor());
    }

    @Test
    public void testCleanupPolicy() {
        assertNull(settings.cleanupPolicy());

        settings.cleanupPolicy(CleanupPolicy.COMPACT);
        assertEquals(CleanupPolicy.COMPACT, settings.cleanupPolicy());

        settings.cleanupPolicy(CleanupPolicy.DELETE);
        assertEquals(CleanupPolicy.DELETE, settings.cleanupPolicy());

        settings.cleanupPolicy(CleanupPolicy.COMPACT_AND_DELETE);
        assertEquals(CleanupPolicy.COMPACT_AND_DELETE, settings.cleanupPolicy());
    }

    @Test
    public void testRetentionTime() {
        assertNull(settings.retentionTime(TimeUnit.MILLISECONDS));
        assertNull(settings.retentionTime(TimeUnit.SECONDS));

        settings.retentionTime(10, TimeUnit.HOURS);
        assertEquals(TimeUnit.HOURS.toMillis(10), settings.retentionTime(TimeUnit.MILLISECONDS).longValue());
        assertEquals(TimeUnit.HOURS.toSeconds(10), settings.retentionTime(TimeUnit.SECONDS).longValue());
        assertEquals(TimeUnit.HOURS.toMinutes(10), settings.retentionTime(TimeUnit.MINUTES).longValue());
        assertEquals(TimeUnit.HOURS.toHours(10), settings.retentionTime(TimeUnit.HOURS).longValue());
    }

    @Test
    public void testRetentionBytes() {
        assertNull(settings.retentionBytes());

        settings.retentionBytes(1000);
        assertEquals(1000L, settings.retentionBytes().longValue());

        settings.with(TopicConfig.RETENTION_BYTES_CONFIG, "foo");
        assertNull(settings.retentionBytes());
    }

    @Test
    public void testMinInSyncReplicas() {
        assertNull(settings.minInSyncReplicas());

        settings.minInSyncReplicas((short)2);
        assertEquals(2, settings.minInSyncReplicas().shortValue());

        settings.with(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "foo");
        assertNull(settings.minInSyncReplicas());
    }

    @Test
    public void testUncleanLeaderElection() {
        assertNull(settings.uncleanLeaderElection());

        settings.uncleanLeaderElection(true);
        assertEquals(true, settings.uncleanLeaderElection());
        settings.uncleanLeaderElection(false);
        assertEquals(false, settings.uncleanLeaderElection());

        settings.with(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, null);
        assertNull(settings.uncleanLeaderElection());
        settings.with(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "NOT A BOOLEAN");
        assertFalse(settings.uncleanLeaderElection());
        settings.with(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.TRUE.toString());
        assertTrue(settings.uncleanLeaderElection());
    }

    @Test
    public void testConfigAndWith() {
        String name = "foo";
        String value = "bar";
        assertNull(settings.config(name));
        settings.with(name, value);
        assertEquals(value, settings.config(name));
        settings.with(name, null);
        assertEquals(null, settings.config(name));
    }

    @Test
    public void testConfigsAndWith() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("foo", "bar");
        configs.put("foo2", "baz");

        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            settings.with(entry.getKey(), entry.getValue());
        }

        assertEquals(configs, settings.configs());

        settings.with(configs);
        assertEquals(configs, settings.configs());
    }

    @Test
    public void clear() {
        settings.cleanupPolicy(CleanupPolicy.COMPACT)
                .minInSyncReplicas((short) 2)
                .with("foo", "bar")
                .replicationFactor((short) 3)
                .partitions(10);
        assertEquals(CleanupPolicy.COMPACT, settings.cleanupPolicy());
        assertEquals(2, settings.minInSyncReplicas().shortValue());
        assertEquals("bar", settings.config("foo"));
        assertEquals(3, settings.replicationFactor());
        assertEquals(10, settings.partitions());

        settings.clear();
        assertNull(settings.cleanupPolicy());
        assertNull(settings.minInSyncReplicas());
        assertNull(settings.config("foo"));
        // Does not unset these ...
        assertEquals(3, settings.replicationFactor());
        assertEquals(10, settings.partitions());
    }

    @Test
    public void toNewTopic() {
        settings.cleanupPolicy(CleanupPolicy.COMPACT)
                .minInSyncReplicas((short) 2)
                .with("foo", "bar")
                .replicationFactor((short) 3)
                .partitions(10);

        NewTopic newTopic = settings.toNewTopic();
        assertEquals(TOPIC_NAME, newTopic.name());
        assertEquals(3, newTopic.replicationFactor());
        assertEquals(10, newTopic.numPartitions());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, newTopic.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("2", newTopic.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
        assertEquals("bar", newTopic.configs().get("foo"));
    }

    @Test
    public void testToString() {
        settings.toString();
        settings.replicationFactor((short)3);
        settings.partitions(500);
        settings.toString();
    }

    @Test
    public void testEquals() {
        settings.cleanupPolicy(CleanupPolicy.COMPACT)
                .minInSyncReplicas((short) 2)
                .with("foo", "bar")
                .replicationFactor((short) 3)
                .partitions(10);

        TopicSettings other = new ConnectTopicSettings(TOPIC_NAME)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .minInSyncReplicas((short) 2)
                .with("foo", "bar")
                .replicationFactor((short) 3)
                .partitions(10);

        TopicSettings diff = new ConnectTopicSettings(TOPIC_NAME)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .minInSyncReplicas((short) 2)
                .with("foo", "bar")
                .with("foo2", "bar2") // extra
                .replicationFactor((short) 3)
                .partitions(10);

        assertEquals(settings, other);
        assertEquals(other, settings);
        assertEquals(settings, settings);
        assertEquals(other, other);
        assertEquals(diff, diff);

        assertNotEquals(settings, diff);
        assertNotEquals(diff, settings);
    }

}