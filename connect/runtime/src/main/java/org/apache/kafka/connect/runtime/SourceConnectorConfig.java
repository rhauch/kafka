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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.topics.TopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SourceConnectorConfig extends ConnectorConfig {

    protected static final String TOPIC_CREATION_GROUP = "Topic Creation";
    protected static final String TOPIC_CREATION_DEFAULT_PREFIX = "topic.creation.default.";

    public static final String TOPIC_CREATION_REPL_FACTOR_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicManager.REPLICATION_FACTOR_PROPERTY_NAME;
    private static final String TOPIC_CREATION_REPL_FACTOR_DISPLAY = "New Topic Replication Factor";
    private static final String TOPIC_CREATION_REPL_FACTOR_DOC = "The replication factor for new topics created when the connector " +
        "writes to topics that do not yet exist. The default is 3, which requires the Kafka cluster have at least 3 nodes.";

    public static final String TOPIC_CREATION_PARTITIONS_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicManager.PARTITIONS_PROPERTY_NAME;
    private static final String TOPIC_CREATION_PARTITIONS_DISPLAY = "New Topic Partition Count";
    private static final String TOPIC_CREATION_PARTITIONS_DOC = "The number of partitions for new topics created when the connector " +
         "writes to topics that do not yet exist. The default is 1.";

    public static final String TOPIC_CREATION_CLEANUP_POLICY_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.CLEANUP_POLICY_CONFIG;
    private static final String TOPIC_CREATION_CLEANUP_POLICY_DISPLAY = "New Topic Cleanup Policy";
    private static final String TOPIC_CREATION_CLEANUP_POLICY_DOC = "The cleanup policy for new topics created when the connector " +
        "writes to topics that do not yet exist. The \"delete\" policy will discard old segments when their retention time or " +
        "size limit has been reached. The \"compact\" policy will enable <a href=\"#compaction\">log compaction</a> on the topic. " +
        "The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_COMPRESSION_TYPE_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.COMPRESSION_TYPE_CONFIG;
    private static final String TOPIC_CREATION_COMPRESSION_TYPE_DISPLAY = "New Topic Cleanup Policy";
    private static final String TOPIC_CREATION_COMPRESSION_TYPE_DOC = "The compression type for new topics created when the " +
        "connector writes to topics that do not yet exist. Permissible values include the standard compression codecs " +
        "('gzip', 'snappy', 'lz4'), 'uncompressed' for no compression; and 'producer' to retain the original compression codec set " +
        "by the producer. The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_RETENTION_BYTES_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.RETENTION_BYTES_CONFIG;
    private static final String TOPIC_CREATION_RETENTION_BYTES_DISPLAY = "New Topic Retention Size";
    private static final String TOPIC_CREATION_RETENTION_BYTES_DOC = "The maximum retention size for new topics created when the " +
        "connector writes to topics that do not yet exist. The \"delete\" setting will discard old log segments to free up space. " +
        "The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_RETENTION_MS_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.RETENTION_MS_CONFIG;
    private static final String TOPIC_CREATION_RETENTION_MS_DISPLAY = "New Topic Retention Time";
    private static final String TOPIC_CREATION_RETENTION_MS_DOC = "The maximum retention time in milliseconds for new topics created when the " +
        "connector writes to topics that do not yet exist and that topic uses the \"delete\" cleanup policy." +
        "The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_DELETE_RETENTION_MS_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.DELETE_RETENTION_MS_CONFIG;
    private static final String TOPIC_CREATION_DELETE_RETENTION_MS_DISPLAY = "New Topic Delete Retention Time";
    private static final String TOPIC_CREATION_DELETE_RETENTION_MS_DOC = "The maximum time in milliseconds to retain tombstones in new topics created when the " +
        "connector writes to topics that do not yet exist and that topic uses the \"compact\" cleanup policy." +
        "The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_MESSAGE_FORMAT_VERSION_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
    private static final String TOPIC_CREATION_MESSAGE_FORMAT_VERSION_DISPLAY = "New Topic Message Format Version";
    private static final String TOPIC_CREATION_MESSAGE_FORMAT_VERSION_DOC = "The message format version for new topics created when the " +
        "connector writes to topics that do not yet exist. The value should be a valid ApiVersion. Some examples are: 0.8.2, 0.9.0.0, and 0.10.0; " +
        "check ApiVersion for more details. The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
    private static final String TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_DISPLAY = "New Topic Message Timestamp Type";
    private static final String TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_DOC = "The message format version for new topics created when the " +
        "connector writes to topics that do not yet exist. The value is either `CreateTime` or `LogAppendTime`. " +
        "The Kafka cluster's default will be used if not specified.";

    public static final String TOPIC_CREATION_MIN_INSYNC_REPLICAS_CONFIG = TOPIC_CREATION_DEFAULT_PREFIX + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
    private static final String TOPIC_CREATION_MIN_INSYNC_REPLICAS_DISPLAY = "New Topic Message Timestamp Type";
    private static final String TOPIC_CREATION_MIN_INSYNC_REPLICAS_DOC = "The minimum number of in-sync replicas for new topics created when the " +
        "connector writes to topics that do not yet exist. The Kafka cluster's default will be used if not specified.";

    static final ConfigDef config;

    static {
        int orderInGroup = 1;
        ConfigDef configDef = ConnectorConfig.configDef();
        configDef.define(
                TOPIC_CREATION_REPL_FACTOR_CONFIG,
                ConfigDef.Type.INT,
                ConfigDef.NO_DEFAULT_VALUE,
                Range.atLeast(1),
                Importance.MEDIUM,
                TOPIC_CREATION_REPL_FACTOR_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_REPL_FACTOR_DISPLAY
        ).define(
                TOPIC_CREATION_PARTITIONS_CONFIG,
                ConfigDef.Type.INT,
                ConfigDef.NO_DEFAULT_VALUE,
                Range.atLeast(1),
                Importance.MEDIUM,
                TOPIC_CREATION_PARTITIONS_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_PARTITIONS_DISPLAY
        ).define(
                TOPIC_CREATION_MIN_INSYNC_REPLICAS_CONFIG,
                ConfigDef.Type.INT,
                ConfigDef.NO_DEFAULT_VALUE,
                Range.atLeast(-1),
                Importance.LOW,
                TOPIC_CREATION_MIN_INSYNC_REPLICAS_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_MIN_INSYNC_REPLICAS_DISPLAY
        ).define(
                TOPIC_CREATION_CLEANUP_POLICY_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ValidString.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE),
                Importance.LOW,
                TOPIC_CREATION_CLEANUP_POLICY_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_CLEANUP_POLICY_DISPLAY
        ).define(
                TOPIC_CREATION_COMPRESSION_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.LOW,
                TOPIC_CREATION_COMPRESSION_TYPE_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_COMPRESSION_TYPE_DISPLAY
        ).define(
                TOPIC_CREATION_RETENTION_BYTES_CONFIG,
                ConfigDef.Type.LONG,
                ConfigDef.NO_DEFAULT_VALUE,
                Range.atLeast(-1),
                Importance.LOW,
                TOPIC_CREATION_RETENTION_BYTES_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_RETENTION_BYTES_DISPLAY
        ).define(
                TOPIC_CREATION_RETENTION_MS_CONFIG,
                ConfigDef.Type.LONG,
                ConfigDef.NO_DEFAULT_VALUE,
                Range.atLeast(-1),
                Importance.LOW,
                TOPIC_CREATION_RETENTION_MS_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_RETENTION_MS_DISPLAY
        ).define(
                TOPIC_CREATION_DELETE_RETENTION_MS_CONFIG,
                ConfigDef.Type.LONG,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.LOW,
                TOPIC_CREATION_DELETE_RETENTION_MS_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_DELETE_RETENTION_MS_DISPLAY
        ).define(
                TOPIC_CREATION_MESSAGE_FORMAT_VERSION_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.LOW,
                TOPIC_CREATION_MESSAGE_FORMAT_VERSION_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_MESSAGE_FORMAT_VERSION_DISPLAY
        ).define(
                TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ValidString.in(TimestampType.CREATE_TIME.toString(), TimestampType.LOG_APPEND_TIME.toString()),
                Importance.LOW,
                TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_DOC,
                TOPIC_CREATION_GROUP,
                orderInGroup++,
                Width.SHORT,
                TOPIC_CREATION_MESSAGE_TIMESTAMP_TYPE_DISPLAY
        );
        config = configDef;
    }

    public static ConfigDef configDef() {
        return config;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public SourceConnectorConfig(Plugins plugins, Map<String, String> props) {
        super(plugins, config, props);
    }

    public boolean usesTopicCreation() {
        return topicCreationDefaultPartitions() != null && topicCreationDefaultReplicationFactor() != null;
    }

    public Integer topicCreationDefaultPartitions() {
        return getInt(TOPIC_CREATION_PARTITIONS_CONFIG);
    }

    public Short topicCreationDefaultReplicationFactor() {
        return getShort(TOPIC_CREATION_REPL_FACTOR_CONFIG);
    }

    public Map<String, Object> topicCreationSettings() {
        return originalsWithPrefix(TOPIC_CREATION_DEFAULT_PREFIX, true);
    }

    public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }
}
