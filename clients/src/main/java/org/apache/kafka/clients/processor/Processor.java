package org.apache.kafka.clients.processor;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

  void init(ProcessorContext context);

  void process(K key, V value);

  void punctuate(long streamTime);

  void close();
}
