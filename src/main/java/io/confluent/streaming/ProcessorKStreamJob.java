package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> extends KStreamTopology implements Processor<K, V> {

  @SuppressWarnings("unchecked")
  @Override
  public void topology() {
    ((KStream<K, V>)from()).process(this);
  }

}
