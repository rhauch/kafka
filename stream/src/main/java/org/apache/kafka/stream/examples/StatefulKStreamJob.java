package org.apache.kafka.stream.examples;


import org.apache.kafka.stream.topology.Processor;
import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;
import org.apache.kafka.stream.kv.Entry;
import org.apache.kafka.stream.kv.InMemoryKeyValueStore;
import org.apache.kafka.stream.kv.KeyValueIterator;
import org.apache.kafka.stream.kv.KeyValueStore;
import org.apache.kafka.stream.topology.SingleProcessorTopology;

import java.util.Properties;

/**
 * Created by guozhang on 7/27/15.
 */

public class StatefulKStreamJob implements Processor<String, Integer> {

  private KStreamContext context;
  private KeyValueStore<String, Integer> kvStore;

  @Override
  public void init(KStreamContext context) {
    this.context = context;
    this.context.schedule(this, 1000);

    this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
  }

  @Override
  public void process(String key, Integer value) {
    Integer oldValue = this.kvStore.get(key);
    if (oldValue == null) {
      this.kvStore.put(key, value);
    } else {
      int newValue = oldValue + value;
      this.kvStore.put(key, newValue);
    }

    context.commit();
  }

  @Override
  public void punctuate(long streamTime) {
    KeyValueIterator<String, Integer> iter = this.kvStore.all();
    while (iter.hasNext()) {
      Entry<String, Integer> entry = iter.next();
      System.out.println("[" + entry.key() + ", " + entry.value() + "]");
    }
  }

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(StatefulKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
  }
}
