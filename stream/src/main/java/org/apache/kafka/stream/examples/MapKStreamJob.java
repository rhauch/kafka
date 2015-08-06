/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream.examples;

import org.apache.kafka.stream.KStream;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KeyValue;
import org.apache.kafka.stream.topology.KeyValueMapper;
import org.apache.kafka.stream.topology.Predicate;

import java.util.Properties;

public class MapKStreamJob extends KStreamTopology {

    @SuppressWarnings("unchecked")
    @Override
    public void topology() {

        // With overridden de-serializer
        KStream stream1 = from(new StringDeserializer(), new StringDeserializer(), "topic1");

        stream1.map(new KeyValueMapper<String, Integer, String, String>() {
            @Override
            public KeyValue<String, Integer> apply(String key, String value) {
                return new KeyValue<>(key, new Integer(value));
            }
        }).filter(new Predicate<String, Integer>() {
            @Override
            public boolean apply(String key, Integer value) {
                return true;
            }
        }).sendTo("topic2");

        // Without overriden de-serialzier
        KStream<String, Integer> stream2 = (KStream<String, Integer>) from("topic2");

        KStream<String, Integer>[] streams = stream2.branch(
            new Predicate<String, Integer>() {
                @Override
                public boolean apply(String key, Integer value) {
                    return true;
                }
            },
            new Predicate<String, Integer>() {
                @Override
                public boolean apply(String key, Integer value) {
                    return true;
                }
            }
        );

        streams[0].sendTo("topic3");
        streams[1].sendTo("topic4");
    }

    public static void main(String[] args) {
        KafkaStreaming kstream = new KafkaStreaming(new MapKStreamJob(), new StreamingConfig(new Properties()));
        kstream.run();
    }
}
