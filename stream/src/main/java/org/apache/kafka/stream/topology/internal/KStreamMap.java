package org.apache.kafka.stream.topology.internal;

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KeyValue;
import org.apache.kafka.stream.topology.KeyValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, V, K1, V1> mapper;

  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamTopology topology) {
    super(topology);
    this.mapper = mapper;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    super.bind(context, KStreamMetadata.unjoinable());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized (this) {
      KeyValue<K, V> newPair = mapper.apply((K1)key, (V1)value);
      forward(newPair.key, newPair.value, timestamp);
    }
  }

}
