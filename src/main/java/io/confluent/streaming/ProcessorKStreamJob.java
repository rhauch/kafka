package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> implements KStreamJob, Processor<K, V> {

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamInitializer initializer) {
    initializer.from().process((Processor) this);
  }

  @Override
  public void close() {
    // do nothing
  }
}
