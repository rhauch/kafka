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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Utility to simplify creating and managing topics via the {@link org.apache.kafka.clients.admin.AdminClient}.
 */
public class TopicAdmin implements AutoCloseable {

    private static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    private static final String CLEANUP_POLICY_COMPACT = "compact";

    private static final String MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas";

    private static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = "unclean.leader.election.enable";

    /**
     * A builder of {@link NewTopic} instances.
     */
    public static class NewTopicBuilder {
        private String name;
        private int numPartitions;
        private short replicationFactor;
        private Map<String, String> configs = new HashMap<>();

        NewTopicBuilder(String name) {
            this.name = name;
        }

        /**
         * Specify the desired number of partitions for the topic.
         *
         * @param numPartitions the desired number of partitions; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder partitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Specify the desired replication factor for the topic.
         *
         * @param replicationFactor the desired replication factor; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Specify that the topic should be compacted.
         *
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder compacted() {
            this.configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
            return this;
        }

        /**
         * Specify the minimum number of in-sync replicas required for this topic.
         *
         * @param minInSyncReplicas the minimum number of in-sync replicas allowed for the topic; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder minInSyncReplicas(short minInSyncReplicas) {
            this.configs.put(MIN_INSYNC_REPLICAS_CONFIG, Short.toString(minInSyncReplicas));
            return this;
        }

        /**
         * Specify whether the broker is allowed to elect a leader that was not an in-sync replica when no ISRs
         * are available.
         *
         * @param allow true if unclean leaders can be elected, or false if they are not allowed
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder uncleanLeaderElection(boolean allow) {
            this.configs.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.toString(allow));
            return this;
        }

        /**
         * Specify the configuration properties for the topic, overwriting any previously-set properties.
         *
         * @param configs the desired topic configuration properties, or null if all existing properties should be cleared
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder config(Map<String, Object> configs) {
            if (configs != null) {
                for (Map.Entry<String, Object> entry : configs.entrySet()) {
                    Object value = entry.getValue();
                    this.configs.put(entry.getKey(), value != null ? value.toString() : null);
                }
            } else {
                this.configs.clear();
            }
            return this;
        }

        /**
         * Build the {@link NewTopic} representation.
         *
         * @return the topic description; never null
         */
        public NewTopic build() {
            return new NewTopic(name, numPartitions, replicationFactor).configs(configs);
        }
    }

    /**
     * Obtain a {@link NewTopicBuilder builder} to define a {@link NewTopic}.
     *
     * @param topicName the name of the topic
     * @return the {@link NewTopic} description of the topic; never null
     */
    public static NewTopicBuilder defineTopic(String topicName) {
        return new NewTopicBuilder(topicName);
    }

    private static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);
    private final Map<String, Object> adminConfig;
    private final AdminClient admin;

    /**
     * Create a new topic admin component with the given configuration.
     *
     * @param adminConfig the configuration for the {@link AdminClient}
     */
    public TopicAdmin(Map<String, Object> adminConfig) {
        this(adminConfig, AdminClient.create(adminConfig));
    }

    // visible for testing
    TopicAdmin(Map<String, Object> adminConfig, AdminClient adminClient) {
        this.admin = adminClient;
        this.adminConfig = adminConfig != null ? adminConfig : Collections.<String, Object>emptyMap();
    }

   /**
     * Attempt to create the topic described by the given definition, returning true if the topic was created or false
     * if the topic already existed.
     *
     * @param topic the specification of the topic
     * @return true if the topic was created or false if the topic already existed.
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     * @throws UnsupportedVersionException if the broker does not support the necessary APIs to perform this request
     */
    public boolean createTopic(NewTopic topic) {
        if (topic == null) return false;
        Set<String> newTopicNames = createTopics(Collections.singleton(topic));
        return newTopicNames.contains(topic.name());
    }

    /**
     * Attempt to create the topics described by the given definitions, returning all of the names of those topics that
     * were created by this request. Any existing topics with the same name are unchanged, and the names of such topics
     * are excluded from the result.
     * <p>
     * Apache Kafka added support for creating topics in 0.10.1.0, so this method works as expected with that and later versions.
     * With brokers older than 0.10.1.0, this method is unable to create topics and always returns an empty set.
     *
     * @param topics the specifications of the topics
     * @return the names of the topics that were created by this operation; never null but possibly empty
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     */
    public Set<String> createTopics(Collection<NewTopic> topics) {
        return createTopics(topics, new CreateTopicResponseHandler());
    }

    /**
     * Attempt to create the topics described by the given definitions, returning all of the names of those topics that
     * were created by this request. Any existing topics with the same name are unchanged, and the names of such topics
     * are excluded from the result.
     * <p>
     * Apache Kafka added support for creating topics in 0.10.1.0, so this method works as expected with that and later versions.
     * With brokers older than 0.10.1.0, this method is unable to create topics and always returns an empty set.
     * <p>
     * The response handler will be called after a topic is created or with any errors that result from attempting to create a topic.
     * The handler is responsible for logging any errors and/or throwing any exceptions, which are not caught by this method. For example,
     * many handlers will wrap {@link TimeoutException} and unknown {@link Throwable} errors in a {@link ConnectException}.
     *
     * @param topics the specifications of the topics; may not be null
     * @param handler the handler for successes and failures; may not be null
     * @return the names of the topics that were created by this operation; never null but possibly empty
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     */
    public Set<String> createTopics(Collection<NewTopic> topics, CreateTopicResponseHandler handler) {
        final LinkedHashMap<String, NewTopic> topicsByName = new LinkedHashMap<>();
        topics.stream().forEach(newTopic -> topicsByName.put(newTopic.name(), newTopic));

        if (topicsByName.isEmpty()) return Collections.emptySet();

        // Attempt to create any missing topics
        CreateTopicsOptions args = new CreateTopicsOptions().validateOnly(false);
        Map<String, KafkaFuture<Void>> newResults = admin.createTopics(topicsByName.values(), args).values();

        // Iterate over each future so that we can handle individual failures like when some topics already exist
        Topics newTopics = new Topics(topicsByName.keySet(), bootstrapServers());
        Set<String> newlyCreatedTopicNames = new HashSet<>();
        for (Map.Entry<String, KafkaFuture<Void>> entry : newResults.entrySet()) {
            String topic = entry.getKey();
            try {
                entry.getValue().get();
                handler.handleCreated(newTopics, topicsByName.get(topic));
                newlyCreatedTopicNames.add(topic);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TopicExistsException) {
                    handler.handleExisting(newTopics, topic);
                } else if (cause instanceof UnsupportedVersionException) {
                    handler.handleError(newTopics, topic, (UnsupportedVersionException) cause);
                } else if (cause instanceof ClusterAuthorizationException) {
                    handler.handleError(newTopics, topic, (ClusterAuthorizationException) cause);
                } else if (cause instanceof TimeoutException) {
                    handler.handleError(newTopics, topic, (TimeoutException) cause);
                } else {
                    handler.handleError(newTopics, topic, cause);
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create/find topic(s) " + newTopics.topicNamesAsString(), e);
            }
        }
        return newlyCreatedTopicNames;
    }

    @Override
    public void close() {
        admin.close();
    }

    private String bootstrapServers() {
        Object servers = adminConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        return servers != null ? servers.toString() : "<unknown>";
    }

    public static class Topics {
        private final List<String> topicNames;
        private final String bootstrapServers;
        public Topics(Collection<String> topicNames, String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            this.topicNames = Collections.unmodifiableList(new ArrayList<>(topicNames));
        }

        public String bootstrapServers() {
            return bootstrapServers;
        }

        public List<String> topicNames() {
            return topicNames;
        }

        public String topicNamesAsString() {
            return topicNames().stream().collect(Collectors.joining("', '","'","'"));
        }

        @Override
        public String toString() {
            return topicNamesAsString() + " on brokers " + bootstrapServers();
        }
    }

    public static class CreateTopicResponseHandler {

        private boolean unsupportedSeen = false;
        private boolean unauthorizedSeen = false;

        public void handleCreated(Topics topics, NewTopic topic) {
            log.info("Created topic {} on brokers at {}", topic, topics.bootstrapServers());
        }

        public void handleExisting(Topics topics, String topicName) {
            log.debug("Found existing topic '{}' on the brokers at {}", topicName, topics.bootstrapServers());
        }

        public void handleError(Topics topics, String topicName, UnsupportedVersionException error) {
            if (!unsupportedSeen) {
                unsupportedSeen = true;
                log.trace("Unsupported version error when creating topic '{}'", topicName, error);
                log.debug("Unable to create topic(s) '{}' since the brokers at {} do not support the CreateTopics API.",
                          " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                          topics.topicNamesAsString(), topics.bootstrapServers());
            }
        }

        public void handleError(Topics topics, String topicName, ClusterAuthorizationException error) {
            if (!unauthorizedSeen) {
                unauthorizedSeen = true;
                log.trace("Cluster authorization failure when creating topic '{}'", topicName, error);
                log.debug("Not authorized to create topic(s) '{}'." +
                          " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                          topics.topicNamesAsString(), topics.bootstrapServers());
            }
        }

        public void handleError(Topics topics, String topicName, TimeoutException error) {
            // Timed out waiting for the operation to complete
            String msg = String.format("Timed out while checking for or creating topic(s) {}." +
                                       " This could indicate a connectivity issue, unavailable topic partitions, or if" +
                                       " this is your first use of the topic it may have taken too long to create.",
                                       topics.topicNamesAsString());
            throw new ConnectException(msg, error);
        }

        public void handleError(Topics topics, String topicName, Throwable error) {
            throw new ConnectException("Error while attempting to create/find topic(s) " + topics.topicNamesAsString(), error);
        }
    }
}
