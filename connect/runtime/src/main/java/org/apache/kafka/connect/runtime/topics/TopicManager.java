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
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin.CreateTopicResponseHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopicManager {

    public static final String REPLICATION_FACTOR_PROPERTY_NAME = "replication.factor";
    public static final String PARTITIONS_PROPERTY_NAME = "partitions";

    private final boolean allowTopicCreation;
    private final Map<String, Object> adminProps;
    private final Set<String> existingTopics = new HashSet<>();
    private final Set<String> assumedExistingTopics = new HashSet<>();
    private final CreateTopicResponseHandler handler;
    private final int partitions;
    private final short replicationFactor;
    private final Map<String, String> topicSettings = new HashMap<>();

    public TopicManager(
            WorkerConfig config,
            SourceConnectorConfig sourceConnectorConfig,
            CreateTopicResponseHandler handler
    ) {
        allowTopicCreation = config.getBoolean(WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG)
                             && sourceConnectorConfig.usesTopicCreation();
        adminProps = config.originals();
        this.handler = handler != null ? handler : new CreateTopicResponseHandler();
        for (Map.Entry<String, Object> entry : sourceConnectorConfig.topicCreationSettings().entrySet()) {
            if (REPLICATION_FACTOR_PROPERTY_NAME.equals(entry.getKey())) {
            }
            if (PARTITIONS_PROPERTY_NAME.equals(entry.getKey())) {
                continue;
            }
            this.topicSettings.put(entry.getKey(), entry.getKey().toString());
        }
        partitions = allowTopicCreation ? sourceConnectorConfig.topicCreationDefaultPartitions() : 0;
        replicationFactor = allowTopicCreation ? sourceConnectorConfig.topicCreationDefaultReplicationFactor() : 0;
        // TODO: Log topic settings and whether topic creation will be done
    }

    public Set<String> createMissingTopics(List<SourceRecord> records) {
        if (!allowTopicCreation) {
            return Collections.emptySet();
        }
        Map<String, NewTopic> newTopics = null;
        for (SourceRecord record : records) {
            String topicName = record.topic();
            if (topicExists(topicName)) {
                continue;
            }
            if (newTopics == null) {
                newTopics = new HashMap<>();
            }
            if (!newTopics.containsKey(topicName)) {
                newTopics.put(topicName, topicSettingsFor(topicName));
            }
        }
        if (newTopics != null) {
            try (TopicAdmin admin = new TopicAdmin(adminProps)) {
                // TODO: Log attempting to create or find topics
                Set<String> newTopicNames = admin.createTopics(newTopics.values());
                existingTopics.addAll(newTopicNames);
                Set<String> assumedExisting = new HashSet<>(newTopics.keySet());
                assumedExisting.removeAll(newTopicNames);
                assumedExistingTopics.addAll(assumedExisting);
                // TODO: Log the topics that were created
                // TODO: TRACE log the topics that are assumed to already exist
                return newTopicNames;
            }
        }
        return Collections.emptySet();
    }

    protected NewTopic topicSettingsFor(String topicName) {
        return new NewTopic(topicName, partitions, replicationFactor).configs(topicSettings);
    }

    public boolean topicExists(String topicName) {
        return existingTopics.contains(topicName) || assumedExistingTopics.contains(topicName);
    }

}
