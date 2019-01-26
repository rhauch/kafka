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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ConnectTopicSettings implements TopicSettings {

    public static final String REPLICATION_FACTOR_PROPERTY_NAME = "replication.factor";
    public static final String PARTITIONS_PROPERTY_NAME = "partitions";

    public static int DEFAULT_NUM_PARTITIONS = 1;
    public static short DEFAULT_REPLICATION_FACTOR = 3;

    static CleanupPolicy parseCleanupPolicy(Object names) {
        if (names == null) {
            return null;
        }
        String strNames = names.toString();
        if (names == null || strNames.trim().isEmpty() ) {
            return null;
        }
        boolean compact = false;
        boolean delete = false;
        for (String name : strNames.split(",")) {
            name = name.trim();
            if (TopicConfig.CLEANUP_POLICY_COMPACT.equalsIgnoreCase(name)) {
                compact = true;
            } else if (TopicConfig.CLEANUP_POLICY_DELETE.equalsIgnoreCase(name)) {
                delete = true;
            }
        }
        if (delete && compact) {
            return CleanupPolicy.COMPACT_AND_DELETE;
        }
        if (compact) {
            return CleanupPolicy.COMPACT;
        }
        if (delete) {
            return CleanupPolicy.DELETE;
        }
        return null;
    }


    private final String name;
    private final Map<String, Object> configs = new HashMap<>();
    private int numPartitions = DEFAULT_NUM_PARTITIONS;
    private short replicationFactor = DEFAULT_REPLICATION_FACTOR;

    public ConnectTopicSettings(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("The topic name may not be null or empty");
        }
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int partitions() {
        return numPartitions;
    }

    @Override
    public short replicationFactor() {
        return replicationFactor;
    }

    @Override
    public CleanupPolicy cleanupPolicy() {
        return parseCleanupPolicy(config(TopicConfig.CLEANUP_POLICY_CONFIG));
    }

    @Override
    public Long retentionTime(TimeUnit unit) {
        Long value = parseNumber(TopicConfig.RETENTION_MS_CONFIG, Long::parseLong);
        return value == null ? null : unit.convert(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public Long retentionBytes() {
        return parseNumber(TopicConfig.RETENTION_BYTES_CONFIG, Long::parseLong);
    }

    @Override
    public Short minInSyncReplicas() {
        return parseNumber(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short::parseShort);
    }

    @Override
    public Boolean uncleanLeaderElection() {
        Object value = config(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
        return value == null ? null : Boolean.parseBoolean(value.toString());
    }

    @Override
    public Object config(String name) {
        return configs.get(name);
    }

    @Override
    public Map<String, Object> configs() {
        return Collections.unmodifiableMap(configs);
    }

    public Map<String, Object> allConfigs() {
        Map<String, Object> allConfigs = new HashMap<>(configs);
        allConfigs.put(PARTITIONS_PROPERTY_NAME, partitions());
        allConfigs.put(REPLICATION_FACTOR_PROPERTY_NAME, replicationFactor());
        return allConfigs;
    }

    @Override
    public ConnectTopicSettings partitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    @Override
    public ConnectTopicSettings replicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }

    @Override
    public ConnectTopicSettings cleanupPolicy(CleanupPolicy policy) {
        String value = null;
        if (policy != null) {
            switch (policy) {
                case COMPACT:
                    value = TopicConfig.CLEANUP_POLICY_COMPACT;
                    break;
                case DELETE:
                    value = TopicConfig.CLEANUP_POLICY_DELETE;
                    break;
                case COMPACT_AND_DELETE:
                    value = TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE;
                    break;
            }
        }
        return with(TopicConfig.CLEANUP_POLICY_CONFIG, value);
    }

    @Override
    public ConnectTopicSettings retentionTime(long time, TimeUnit unit) {
        return with(TopicConfig.RETENTION_MS_CONFIG, Long.toString(unit.toMillis(time)));
    }

    @Override
    public ConnectTopicSettings retentionBytes(long bytes) {
        return with(TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(bytes));
    }

    @Override
    public ConnectTopicSettings minInSyncReplicas(short minInSyncReplicas) {
        return with(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minInSyncReplicas));
    }

    @Override
    public ConnectTopicSettings uncleanLeaderElection(boolean allow) {
        return with(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.toString(allow));
    }

    @Override
    public ConnectTopicSettings with(String configName, Object configValue) {
        if (REPLICATION_FACTOR_PROPERTY_NAME.equals(configName)) {
            Short value = parseNumber(configValue, Short::parseShort);
            if (value != null) {
                replicationFactor(value.shortValue());
            }
        } else if (PARTITIONS_PROPERTY_NAME.equals(configName)) {
            Integer value = parseNumber(configValue, Integer::parseInt);
            if (value != null) {
                partitions(value.intValue());
            }
        } else {
            configs.put(configName, configValue);
        }
        return this;
    }

    @Override
    public ConnectTopicSettings with(Map<String, Object> configs) {
        configs.forEach(this::with);
        return this;
    }

    @Override
    public ConnectTopicSettings clear() {
        configs.clear();
        return this;
    }

    /**
     * Create a copy of this object that uses the specified name for the topic.
     *
     * @param name the new topic name
     * @return a copy of this object but with the new name; never null
     */
    public ConnectTopicSettings withTopicName(String name) {
        return new ConnectTopicSettings(name)
                       .replicationFactor(this.replicationFactor())
                       .partitions(this.partitions())
                       .with(configs);
    }

    public NewTopic toNewTopic() {
        NewTopic result = new NewTopic(name, numPartitions, replicationFactor);
        Map<String, String> strConfigs = new HashMap<>();
        configs.entrySet().forEach(e->strConfigs.put(e.getKey(), e.getValue() != null ? e.getValue().toString() : null));
        result.configs(strConfigs);
        return result;
    }

    @Override
    public String toString() {
        return toNewTopic().toString();
    }

    @Override
    public int hashCode() {
        return name().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ConnectTopicSettings) {
            ConnectTopicSettings that = (ConnectTopicSettings) obj;
            return this.name().equals(that.name())
                    && this.replicationFactor() == that.replicationFactor()
                    && this.partitions() == that.partitions()
                    && this.configs().equals(that.configs());
        }
        return false;
    }

    <T extends Number> T parseNumber(String name, Function<String, T> parser) {
        Object value = config(name);
        T parsed = null;
        if (value != null) {
            try {
                parsed = parser.apply(value.toString());
            } catch (NumberFormatException e) {
                parsed = null;
            }
        }
        return parsed;
    }

    <T extends Number> T parseNumber(Object value, Function<String, T> parser) {
        T parsed = null;
        if (value != null) {
            try {
                parsed = parser.apply(value.toString());
            } catch (NumberFormatException e) {
                parsed = null;
            }
        }
        return parsed;
    }
}
