package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> extends KStreamTopology implements Processor<K, V> {

  private final String[] topics;

  public ProcessorKStreamJob(String... topics) {
    this.topics = topics;
  }
  @SuppressWarnings("unchecked")
  @Override
  public void topology() {
    ((KStream<K, V>)from(topics)).process(this);
  }

}
