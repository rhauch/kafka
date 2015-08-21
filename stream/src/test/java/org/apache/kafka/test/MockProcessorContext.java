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

package org.apache.kafka.test;

import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.RecordCollector;
import org.apache.kafka.streaming.processor.RestoreFunc;
import org.apache.kafka.streaming.processor.StateStore;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;

public class MockProcessorContext implements ProcessorContext {

    Serializer serializer;
    Deserializer deserializer;

    long timestamp = -1L;

    public MockProcessorContext(Serializer<?> serializer, Deserializer<?> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean joinable(ProcessorContext other) {
        // TODO
        return true;
    }

    @Override
    public int id() {
        return -1;
    }

    @Override
    public Serializer<?> keySerializer() {
        return serializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return serializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return deserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return deserializer;
    }

    @Override
    public RecordCollector recordCollector() {
        throw new UnsupportedOperationException("recordCollector() not supported.");
    }

    @Override
    public File stateDir() {
        throw new UnsupportedOperationException("stateDir() not supported.");
    }

    @Override
    public Metrics metrics() {
        throw new UnsupportedOperationException("metrics() not supported.");
    }

    @Override
    public void register(StateStore store, RestoreFunc func) {
        throw new UnsupportedOperationException("restore() not supported.");
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("flush() not supported.");
    }

    @Override
    public void send(String topic, Object key, Object value) {
        throw new UnsupportedOperationException("send() not supported.");
    }

    @Override
    public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
        throw new UnsupportedOperationException("send() not supported.");
    }

    @Override
    public void schedule(KafkaProcessor processor, long interval) {
        throw new UnsupportedOperationException("schedule() not supported");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("topic() not supported.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("partition() not supported.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("offset() not supported.");
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

}
