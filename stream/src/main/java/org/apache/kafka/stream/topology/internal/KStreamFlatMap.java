package org.apache.kafka.stream.topology.internal;

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KeyValue;
import org.apache.kafka.stream.topology.KeyValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper;

  KStreamFlatMap(KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper, KStreamTopology topology) {
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
    synchronized(this) {
      KeyValue<K, ? extends Iterable<V>> newPair = mapper.apply((K1)key, (V1)value);
      for (V v : newPair.value) {
        forward(newPair.key, v, timestamp);
      }
    }
  }

}
