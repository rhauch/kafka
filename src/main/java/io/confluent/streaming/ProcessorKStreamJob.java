package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> implements KStreamJob, Processor<K, V> {

  protected KStreamContext streamContext;

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamContext context) {
    this.streamContext = context;
    context.from().process((Processor) this);
  }

  @Override
  public void close() {
    // do nothing
  }
}
