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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.connect.util.TopicAdmin.CreateTopicResponseHandler;
import org.apache.kafka.connect.util.TopicAdmin.Topics;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicAdminTest {

    private CustomResponseHandler handler;
    private Set<String> createdTopicNames;

    @Before
    public void setup() {
        handler = new CustomResponseHandler();
        createdTopicNames = new HashSet<>();
    }

    /**
     * 0.11.0.0 clients can talk with older brokers, but the CREATE_TOPIC API was added in 0.10.1.0. That means,
     * if our TopicAdmin talks to a pre 0.10.1 broker, it should receive an UnsupportedVersionException, should
     * create no topics, and return false.
     */
    @Test
    public void returnNullWithApiVersionMismatch() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void returnNullWithClusterAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.nodes().iterator().next());
            env.kafkaClient().prepareResponse(createTopicResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void shouldNotCreateTopicWhenItAlreadyExists() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
            mockAdminClient.addTopic(false, "myTopic", Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            assertFalse(admin.createTopic(newTopic));
        }
    }

    @Test
    public void shouldCreateTopicWhenItDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            assertTrue(admin.createTopic(newTopic));
        }
    }

    @Test
    public void shouldCreateOneTopicWhenProvidedMultipleDefinitionsWithSameTopicName() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            Set<String> newTopicNames = admin.createTopics(Arrays.asList(newTopic1, newTopic2));
            assertEquals(1, newTopicNames.size());
            assertEquals(newTopic2.name(), newTopicNames.iterator().next());
        }
    }

    @Test
    public void shouldReturnFalseWhenSuppliedNullTopicDescription() {
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            boolean created = admin.createTopic(null);
            assertFalse(created);
        }
    }

    @Test
    public void handleApiVersionMismatch() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertTrue(createdTopicNames.isEmpty());
            assertTrue(handler.createdTopicNames.isEmpty());
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(1, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleClusterAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.nodes().iterator().next());
            env.kafkaClient().prepareResponse(createTopicResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertTrue(createdTopicNames.isEmpty());
            assertTrue(handler.createdTopicNames.isEmpty());
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(1, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleTimeoutFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.nodes().iterator().next());
            env.kafkaClient().prepareResponse(createTopicResponseWithTimeoutException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertTrue(createdTopicNames.isEmpty());
            assertTrue(handler.createdTopicNames.isEmpty());
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(1, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleUnknownFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNode(cluster.nodes().iterator().next());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnknownException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertTrue(createdTopicNames.isEmpty());
            assertTrue(handler.createdTopicNames.isEmpty());
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(1, handler.unknownErrors);
        }
    }

    @Test
    public void handleExistingTopic() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
            mockAdminClient.addTopic(false, "myTopic", Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertTrue(createdTopicNames.isEmpty());
            assertTrue(handler.createdTopicNames.isEmpty());
            assertEquals(setOf("myTopic"), handler.existingTopicNames);
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleNewTopic() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            createdTopicNames = admin.createTopics(Collections.singleton(newTopic), handler);
            assertEquals(setOf("myTopic"), createdTopicNames);
            assertEquals(setOf("myTopic"), handler.createdTopicNames);
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleNewTopicWhenProvidedMultipleDefinitionsWithSameName() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            createdTopicNames = admin.createTopics(Arrays.asList(newTopic1, newTopic2), handler);
            assertEquals(setOf("myTopic"), createdTopicNames);
            assertEquals(setOf("myTopic"), handler.createdTopicNames);
            assertTrue(handler.existingTopicNames.isEmpty());
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }

    @Test
    public void handleMixtureOfNewAndExistingTopics() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic1").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic2").partitions(2).build();
        NewTopic newTopic3 = TopicAdmin.defineTopic("myTopic3").partitions(3).build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
            mockAdminClient.addTopic(false, "myTopic3", Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            createdTopicNames = admin.createTopics(Arrays.asList(newTopic1, newTopic2, newTopic3), handler);
            assertEquals(setOf("myTopic1", "myTopic2"), createdTopicNames);
            assertEquals(setOf("myTopic1", "myTopic2"), handler.createdTopicNames);
            assertEquals(setOf("myTopic3"), handler.existingTopicNames);
            assertEquals(0, handler.unsupportedVersionErrors);
            assertEquals(0, handler.clusterAuthorizationErrors);
            assertEquals(0, handler.timeoutErrors);
            assertEquals(0, handler.unknownErrors);
        }
    }
    private Cluster createCluster(int numNodes) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));
        return cluster;
    }

    private CreateTopicsResponse createTopicResponseWithUnsupportedVersion(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithClusterAuthorizationException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithTimeoutException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.REQUEST_TIMED_OUT, "Request timed out"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithUnknownException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.UNKNOWN_SERVER_ERROR, "Unknown error"), topics);
    }

    private CreateTopicsResponse createTopicResponse(ApiError error, NewTopic... topics) {
        if (error == null) error = new ApiError(Errors.NONE, "");
        Map<String, ApiError> topicResults = new HashMap<>();
        for (NewTopic topic : topics) {
            topicResults.put(topic.name(), error);
        }
        return new CreateTopicsResponse(topicResults);
    }

    private <T> Set<T> setOf(T...names) {
        return new HashSet<>(Arrays.asList(names));
    }

    protected static class CustomResponseHandler extends CreateTopicResponseHandler {
        protected Set<String> createdTopicNames = new HashSet<>();
        protected Set<String> existingTopicNames = new HashSet<>();
        protected int clusterAuthorizationErrors = 0;
        protected int unsupportedVersionErrors = 0;
        protected int timeoutErrors = 0;
        protected int unknownErrors = 0;

        @Override
        public void handleCreated(Topics topics, NewTopic topic) {
            createdTopicNames.add(topic.name());
        }

        @Override
        public void handleExisting(Topics topics, String topicName) {
            existingTopicNames.add(topicName);
        }

        @Override
        public void handleError(Topics topics, String topicName, ClusterAuthorizationException error) {
            clusterAuthorizationErrors++;
        }

        @Override
        public void handleError(Topics topics, String topicName, UnsupportedVersionException error) {
            unsupportedVersionErrors++;
        }

        @Override
        public void handleError(Topics topics, String topicName, TimeoutException error) {
            timeoutErrors++;
        }

        @Override
        public void handleError(Topics topics, String topicName, Throwable error) {
            unknownErrors++;
        }
    }
}
