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
import org.apache.kafka.connect.errors.TopicCreationFailedException;
import org.apache.kafka.connect.storage.TopicSettings;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A class that manages a set of topics and the rules for creating them.
 */
public class TopicCreationRules {

    public interface TopicCreator {
        boolean createTopic(NewTopic settings);
    }

    private final Set<String> existingTopics = new HashSet<>();
    private final Set<String> assumedExistingTopics = new HashSet<>();
    private final TopicCreator createTopicFunction;
    private final Predicate<String> topicExistsFunction;
    private int partitions;
    private short replicationFactor;
    private Map<String, String> topicSettings;
    private boolean enabled = false;

    public TopicCreationRules(TopicCreator createTopicFunction, Predicate<String> topicExistsFunction) {
        Objects.nonNull(createTopicFunction);
        this.createTopicFunction = createTopicFunction != null ? createTopicFunction : (name -> false);
        this.topicExistsFunction = topicExistsFunction;
    }

    public TopicCreationRules with(Map<String, String> config) {
        enabled = config.get()
    }

    public TopicCreationRules defaultPartitions(int partitions) {
        this.partitions = partitions;
        return this;
    }

    public TopicCreationRules defaultReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }

    public void addRule(String ruleName, String topicNameRegex, Map<String, Object> properties) {
        rules.add(new Rule(ruleName, topicNameRegex, properties));
    }

    public boolean topicExists(String topicName) {
        return existingTopics.contains(topicName) || assumedExistingTopics.contains(topicName);
    }

    /**
     * Attempt to create the topic with the specified name. This method returns {@code true} if this call resulted in creating a new topic,
     * or {@code false} if the topic already existed.
     *
     * @param topicName the name of the topic to be created; may not be null
     * @return true if the topic was created, or false if the topic was not created
     * @throws TopicCreationFailedException if the connector was configured to disallow creation of the topic
     */
    public boolean createTopic(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("The topic name may not be null or empty");
        }
        if (topicExists(topicName)) {
            return false;
        }
        // Check whether the topic exists ...
        if (topicExistsFunction.test(topicName)) {
            return false;
        }
        // Find the first rule that matches ...
        Optional<Rule> matchingRule = ruleFor(topicName);
        if (matchingRule.isPresent()) {
            NewTopic settings = new NewTopic(topicName, partitions, replicationFactor);
            settings.configs(topicSettings);
            boolean created = createTopicFunction.createTopic(settings);
            existingTopics.add(topicName);
            return created;
        } else {
            assumedExistingTopics.add(topicName);
        }
        return false;
    }

    protected int ruleCount() {
        return rules.size();
    }

    protected Optional<Rule> ruleFor(String topicName) {
        return rules.stream().filter(r -> r.matches(topicName)).findFirst();
    }

    protected static class Rule {

        private final Pattern topicNamePattern;
        private ConnectTopicSettings settings;

        public Rule(String ruleName, String topicNameRegex, Map<String, Object> topicConfigs) {
            topicNamePattern = Pattern.compile(topicNameRegex);
            settings = new ConnectTopicSettings(ruleName).with(topicConfigs); // store rule name as topic name
        }

        public String name() {
            return settings.name();
        }

        public boolean matches(String topicName) {
            return topicNamePattern.matcher(topicName).matches();
        }

        public NewTopic settingsForTopic(String name) {
            return settings.withTopicName(name);
        }
    }
}
