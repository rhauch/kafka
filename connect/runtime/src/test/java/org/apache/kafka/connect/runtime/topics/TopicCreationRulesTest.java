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

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.topics.TopicCreationRules.Rule;
import org.apache.kafka.connect.storage.TopicSettings;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

public class TopicCreationRulesTest {

    private TopicCreationRules rules;
    private Set<String> existingTopics = new HashSet<>();
    private List<TopicSettings> createdTopics = new ArrayList<>();

    @Before
    public void setup() {
        rules = new TopicCreationRules(this::createTopic, this::topicExists);
        existingTopics.add("existing");
    }

    /**
     * {@link org.apache.kafka.connect.runtime.SourceConnectorConfig} has a validator that prevents duplicate names, but we do allow them
     * at this level.
     */
    @Test
    public void shouldAllowAddingMultipleRulesWithSameName() {
        addRule("one", settings("rule1").partitions(500).replicationFactor((short)3).with("foo", "bar1"));
        assertEquals(1, rules.ruleCount());
        addRule("two", settings("rule1").partitions(50).replicationFactor((short)4).with("foo", "bar2"));
        assertEquals(2, rules.ruleCount());
    }

    @Test
    public void shouldFindExistingTopic() {
        assertTopicCreateCalls(0);
        assertTrue(topicExists("existing"));
        assertFalse(rules.createTopic("existing"));
        assertTrue(topicExists("existing"));
        assertTopicCreateCalls(0);
    }

    @Test
    public void shouldNotCreateNewTopicWhenNoRules() {
        assertTopicCreateCalls(0);
        assertFalse(topicExists("non-existing"));
        assertFalse(rules.topicExists("non-existing"));
        for (int i=0; i!=10; ++i) {
            assertFalse(rules.createTopic("non-existing"));
        }
        assertTrue(rules.topicExists("non-existing"));
        assertFalse(topicExists("non-existing"));
        assertTopicCreateCalls(0);
    }

    @Test
    public void shouldCreateNewTopicMatchingTheOnlyPattern() {
        addRule("one", settings("rule1").partitions(500).replicationFactor((short)3).with("foo", "bar1"));

        assertFalse(topicExists("one"));
        assertFalse(rules.topicExists("one"));
        assertTrue(rules.createTopic("one"));
        assertTrue(rules.topicExists("one"));
        assertTopicCreateCalls(1);
        TopicSettings usedSettings = lastSettings();
        assertEquals(500, usedSettings.partitions());
        assertEquals(3, usedSettings.replicationFactor());
        assertEquals(null, usedSettings.minInSyncReplicas());
        assertEquals(null, usedSettings.cleanupPolicy());
        assertEquals(null, usedSettings.retentionBytes());
        assertEquals(null, usedSettings.uncleanLeaderElection());
        assertEquals("bar1", usedSettings.config("foo"));
        assertEquals(null, usedSettings.config("other"));
    }

    @Test
    public void shouldCreateNewTopicsMatchingPatterns() {
        addRule("one", settings("rule1").partitions(500).replicationFactor((short)3).with("foo", "bar1"));
        addRule("two", settings("rule2").partitions(50).replicationFactor((short)4).with("foo", "bar2"));
        addRule("car", settings("rule3").partitions(100).replicationFactor((short)5).with("foo", "bar3"));
        addRule(".*", settings("rule4").partitions(200).replicationFactor((short)6).with("foo", "bar4"));

        assertFalse(topicExists("one"));
        assertFalse(rules.topicExists("one"));
        assertTrue(rules.createTopic("one"));
        assertTrue(rules.topicExists("one"));
        assertTopicCreateCalls(1);
        TopicSettings usedSettings = lastSettings();
        assertEquals(500, usedSettings.partitions());
        assertEquals(3, usedSettings.replicationFactor());
        assertEquals(null, usedSettings.minInSyncReplicas());
        assertEquals(null, usedSettings.cleanupPolicy());
        assertEquals(null, usedSettings.retentionBytes());
        assertEquals(null, usedSettings.uncleanLeaderElection());
        assertEquals("bar1", usedSettings.config("foo"));
        assertEquals(null, usedSettings.config("other"));

        assertFalse(topicExists("car"));
        assertFalse(rules.topicExists("car"));
        assertTrue(rules.createTopic("car"));
        assertTrue(rules.topicExists("car"));
        assertTopicCreateCalls(2);
        usedSettings = lastSettings();
        assertEquals(100, usedSettings.partitions());
        assertEquals(5, usedSettings.replicationFactor());
        assertEquals(null, usedSettings.minInSyncReplicas());
        assertEquals(null, usedSettings.cleanupPolicy());
        assertEquals(null, usedSettings.retentionBytes());
        assertEquals(null, usedSettings.uncleanLeaderElection());
        assertEquals("bar3", usedSettings.config("foo"));
        assertEquals(null, usedSettings.config("other"));

        assertTrue(rules.topicExists("car"));
        assertFalse(rules.createTopic("car"));
        assertTrue(rules.topicExists("car"));
        assertTopicCreateCalls(2);

        assertFalse(topicExists("else"));
        assertFalse(rules.topicExists("else"));
        assertTrue(rules.createTopic("else"));
        assertTrue(rules.topicExists("else"));
        assertTopicCreateCalls(3);
        usedSettings = lastSettings();
        assertEquals(200, usedSettings.partitions());
        assertEquals(6, usedSettings.replicationFactor());
        assertEquals(null, usedSettings.minInSyncReplicas());
        assertEquals(null, usedSettings.cleanupPolicy());
        assertEquals(null, usedSettings.retentionBytes());
        assertEquals(null, usedSettings.uncleanLeaderElection());
        assertEquals("bar4", usedSettings.config("foo"));
        assertEquals(null, usedSettings.config("other"));
    }

    @Test
    public void shouldMatchRulesWithOnlyWildcardRule() {
        addRule(".*", settings("rule4").partitions(100).replicationFactor((short)5).with("foo", "bar2"));

        assertRuleMatches("one", "rule4");
        assertRuleMatches("two", "rule4");
        assertRuleMatches("car", "rule4");
        assertRuleMatches("fad", "rule4");
    }

    @Test
    public void shouldMatchRulesWithMultipleRulesAndWildcardRule() {
        addRule("one(\\d)*", settings("rule1").partitions(500).replicationFactor((short)3).with("foo", "bar1"));
        addRule("two", settings("rule2").partitions(50).replicationFactor((short)4).with("foo", "bar2"));
        addRule("car", settings("rule3").partitions(100).replicationFactor((short)5).with("foo", "bar2"));
        addRule(".*", settings("rule4").partitions(100).replicationFactor((short)5).with("foo", "bar2"));

        assertRuleMatches("one", "rule1");
        assertRuleMatches("one1", "rule1");
        assertRuleMatches("one22", "rule1");
        assertRuleMatches("one333", "rule1");
        assertRuleMatches("two", "rule2");
        assertRuleMatches("two1", "rule4");
        assertRuleMatches("two22", "rule4");
        assertRuleMatches("car", "rule3");
        assertRuleMatches("fad", "rule4");
    }

    @Test
    public void shouldMatchRulesWithMultipleRulesAndNoWildcardRule() {
        addRule("one(\\d)*", settings("rule1").partitions(500).replicationFactor((short)3).with("foo", "bar1"));
        addRule("two", settings("rule2").partitions(50).replicationFactor((short)4).with("foo", "bar2"));
        addRule("car", settings("rule3").partitions(100).replicationFactor((short)5).with("foo", "bar2"));

        assertRuleMatches("one", "rule1");
        assertRuleMatches("one1", "rule1");
        assertRuleMatches("one22", "rule1");
        assertRuleMatches("one333", "rule1");
        assertRuleMatches("two", "rule2");
        assertRuleMatches("car", "rule3");
        assertNoRulesMatch("two1");
        assertNoRulesMatch("two22");
        assertNoRulesMatch("fad");
    }

    @Test
    public void shouldMatchNoRulesWhenNoRulesExist() {
        assertNoRulesMatch("one");
        assertNoRulesMatch("two");
        assertNoRulesMatch("car");
        assertNoRulesMatch("fad");
    }

    protected void assertRuleMatches(String topicName, String ruleName) {
        Optional<Rule> rule = rules.ruleFor(topicName);
        assertTrue(rule.isPresent());
        assertEquals(rule.get().name(), ruleName);
    }

    protected void assertNoRulesMatch(String topicName) {
        Optional<Rule> rule = rules.ruleFor(topicName);
        assertFalse(rule.isPresent());
    }

    protected void assertTopicCreateCalls(int expected) {
        assertEquals(expected, createdTopics.size());
    }

    protected void addRule(String regex, ConnectTopicSettings settings) {
        rules.addRule(settings.name(), regex, settings.allConfigs());
    }

    protected ConnectTopicSettings settings(String ruleName) {
        return new ConnectTopicSettings(ruleName);
    }

    protected TopicSettings lastSettings() {
        return createdTopics.isEmpty() ? null : createdTopics.get(createdTopics.size()-1);
    }

    public boolean topicExists(String topicName) {
        return existingTopics.contains(topicName);
    }

    public boolean createTopic(TopicSettings settings) {
        if (topicExists(settings.name())) {
            return false;
        }
        createdTopics.add(settings);
        return true;
    }

}