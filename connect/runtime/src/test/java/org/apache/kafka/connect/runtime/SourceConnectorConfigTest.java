package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.topics.TopicCreationRules;
import org.apache.kafka.connect.storage.TopicSettings;
import org.apache.kafka.connect.storage.TopicSettings.CleanupPolicy;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class SourceConnectorConfigTest {

    public static final Plugins PLUGINS = new Plugins(Collections.emptyMap());

    private Set<String> preExistingTopics = new HashSet<>();
    private Map<String, TopicSettings> createdTopicsByName = new LinkedHashMap<>();
    private TopicCreationRules rules;

    @Before
    public void setup() {
        preExistingTopics.add("MyPrefix.Existing");
        preExistingTopics.add("OtherExisting");
    }

    @Test
    public void topicCreationRules() throws Exception {
        assertAllExistingTopics("MyPrefix.Existing", "OtherExisting");

        Map<String, String> props = new HashMap<>();
        props.put("name", "MyConnector");
        props.put("connector.class", MockSourceConnector.class.getName());

        props.put("topic.creation", "firstRule,defaultRule");
        props.put("topic.creation.firstRule.regex", "MyPrefix.*");
        props.put("topic.creation.firstRule.replication.factor", "3");
        props.put("topic.creation.firstRule.partitions", "5");
        props.put("topic.creation.firstRule.cleanup.policy", "compact");
        props.put("topic.creation.firstRule.min.insync.replicas", "2");
        props.put("topic.creation.firstRule.unclean.leader.election.enable", "false");
        props.put("topic.creation.defaultRule.regex", ".*");
        props.put("topic.creation.defaultRule.replication.factor", "4");
        props.put("topic.creation.defaultRule.partitions", "1");
        props.put("topic.creation.defaultRule.cleanup.policy", "delete");
        props.put("topic.creation.defaultRule.min.insync.replicas", "2");
        props.put("topic.creation.defaultRule.unclean.leader.election.enable", "true");
        props.put("topic.creation.defaultRule.extra.property", "foo");

        SourceConnectorConfig config = new SourceConnectorConfig(PLUGINS, props);
        rules = config.topicCreationRules(this::createTopic, this::topicExists);

        // Create the topic that does not yet exist
        assertTopicCreationCalls(0);
        assertNonExistingTopics("MyPrefix.TopicA");
        assertTrue(rules.createTopic("MyPrefix.TopicA"));
        assertAllExistingTopics("MyPrefix.Existing", "OtherExisting", "MyPrefix.TopicA");
        assertTopicCreationCalls(1);

        // Check the topic settings that were used to create the topic
        TopicSettings settings = settingsForNewTopic("MyPrefix.TopicA");
        assertEquals(3, settings.replicationFactor());
        assertEquals(5, settings.partitions());
        assertEquals(CleanupPolicy.COMPACT, settings.cleanupPolicy());
        assertEquals(2, (short)settings.minInSyncReplicas());
        assertEquals(false, settings.uncleanLeaderElection());
        assertEquals(null, settings.retentionBytes());
        assertEquals(null, settings.config("extra.property"));
        assertEquals(null, settings.config("non.existent.property"));

        // Verify that creating it again does nothing ...
        assertFalse(rules.createTopic("MyPrefix.TopicA"));
        assertTopicCreationCalls(1);


        // Create a second topic that does not yet exist
        assertTopicCreationCalls(1);
        assertNonExistingTopics("SomeOtherTopic");
        assertTrue(rules.createTopic("SomeOtherTopic"));
        assertAllExistingTopics("MyPrefix.Existing", "OtherExisting", "MyPrefix.TopicA", "SomeOtherTopic");
        assertTopicCreationCalls(2);

        // Check the topic settings that were used to create the topic
        settings = settingsForNewTopic("SomeOtherTopic");
        assertEquals(4, settings.replicationFactor());
        assertEquals(1, settings.partitions());
        assertEquals(CleanupPolicy.DELETE, settings.cleanupPolicy());
        assertEquals(2, (short)settings.minInSyncReplicas());
        assertEquals(true, settings.uncleanLeaderElection());
        assertEquals(null, settings.retentionBytes());
        assertEquals("foo", settings.config("extra.property"));
        assertEquals(null, settings.config("non.existent.property"));

        // Verify that creating it again does nothing ...
        assertFalse(rules.createTopic("SomeOtherTopic"));
        assertTopicCreationCalls(2);

    }

    protected TopicSettings settingsForNewTopic(String topicName) {
        return createdTopicsByName.get(topicName);
    }

    protected void assertNonExistingTopics(String... topicNames) {
        for (String topicName : topicNames) {
            assertFalse(topicExists(topicName));
        }
    }

    protected void assertAllExistingTopics(String... topicNames) {
        Set<String> expectedNames = new HashSet<>(Arrays.asList(topicNames));
        Set<String> existingNames = new HashSet<>(preExistingTopics);
        existingNames.addAll(createdTopicsByName.keySet());
        assertEquals(expectedNames, existingNames);
    }

    public boolean topicExists(String topicName) {
        return preExistingTopics.contains(topicName) || createdTopicsByName.containsKey(topicName);
    }

    public boolean createTopic(TopicSettings settings) {
        if (topicExists(settings.name())) {
            return false;
        }
        createdTopicsByName.put(settings.name(), settings);
        return true;
    }

    protected void assertTopicCreationCalls(int expected) {
        assertEquals(expected, createdTopicsByName.size());
    }
}