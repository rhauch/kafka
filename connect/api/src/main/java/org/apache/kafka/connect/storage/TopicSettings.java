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
package org.apache.kafka.connect.storage;

import java.util.Map;

/**
 * Topic-specific settings for a new topic.
 */
public interface TopicSettings {
    /**
     * The log cleanup policy for segments beyond the retention window
     */
    enum CleanupPolicy {
        /**
         * Ensures that Kafka will always retain at least the last known value for each message key within the log of data for a single topic partition.
         */
        COMPACT,
        /**
         * Discard old log data after a fixed period of time or when the log reaches some predetermined size.
         */
        DELETE,
        /**
         * {@link #COMPACT Compact} to retain at least the last known value for each message key <i>and</i>
         * {@link #DELETE delete} messages after a period of time.
         */
        COMPACT_AND_DELETE;
    }

    /**
     * Get the name of the topic.
     * @return the name of the topic
     */
    String name();

    /**
     * Get the number of partitions.
     * @return the number of partitions; always positive
     */
    int partitions();

    /**
     * Get the replication factor.
     * @return the replication factor; always positive
     */
    short replicationFactor();

    /**
     * Get the cleanup policy.
     * @return the cleanup policy; may be null if the broker's default setting for new topics is to be used
     */
    CleanupPolicy cleanupPolicy();

    /**
     * Get the minimum number of in-sync replicas that must exist for the topic to remain available.
     * @return the minimum number of in-sync replicas; may be null if the broker's default setting for new topics is to be used
     */
    Short minInSyncReplicas();

    /**
     * Get whether the broker is allowed to elect an unclean leader for the topic.
     * @return true if unclean leader election is allowed (potentially leading to data loss), or false otherwise;
     * may be null if the broker's default setting for new topics is to be used
     */
    Boolean uncleanLeaderElection();

    /**
     * Get the value for the named topic-specific configuration.
     * @param name the name of the topic-specific configuration
     * @return the configuration value, or null if the specified configuration has not been set
     */
    Object config(String name);

    /**
     * Get the values for the set topic-specific configuration.
     * @return the map of configuration values keyed by configuration name; never null
     */
    Map<String, Object> config();

    /**
     * Specify the desired number of partitions for the topic.
     *
     * @param numPartitions the desired number of partitions; must be positive
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings partitions(int numPartitions);

    /**
     * Specify the desired replication factor for the topic.
     *
     * @param replicationFactor the desired replication factor; must be positive
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings replicationFactor(short replicationFactor);

    /**
     * Specify the desired cleanup policy for the topic.
     * @param policy the cleanup policy; may not be null
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings cleanupPolicy(CleanupPolicy policy);

    /**
     * Specify the minimum number of in-sync replicas required for this topic.
     *
     * @param minInSyncReplicas the minimum number of in-sync replicas allowed for the topic; must be positive
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings minInSyncReplicas(short minInSyncReplicas);

    /**
     * Specify whether the broker is allowed to elect a leader that was not an in-sync replica when no ISRs
     * are available.
     *
     * @param allow true if unclean leaders can be elected, or false if they are not allowed
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings uncleanLeaderElection(boolean allow);

    /**
     * Specify the configuration properties for the topic, overwriting any previously-set properties.
     *
     * @param configName the name of the topic-specific configuration property
     *                  @param configValue the value for the topic-specific configuration property
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings config(String configName, Object configValue);

    /**
     * Specify the configuration properties for the topic, overwriting all previously-set properties,
     * including {@link #cleanupPolicy(CleanupPolicy)}, {@link #minInSyncReplicas(short)}, and {@link #uncleanLeaderElection(boolean)}.
     *
     * @param configs the desired topic configuration properties, or null if all existing properties should be cleared
     * @return this settings object to allow methods to be chained; never null
     */
    TopicSettings config(Map<String, Object> configs);
}
