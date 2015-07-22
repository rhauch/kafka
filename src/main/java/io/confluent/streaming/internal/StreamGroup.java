package io.confluent.streaming.internal;

import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.util.MinTimestampTracker;
import io.confluent.streaming.util.ParallelExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A StreamGroup is composed of multiple streams from different topics that need to be synchronized.
 */
public class StreamGroup implements ParallelExecutor.Task {

  private final String name;
  private final Ingestor ingestor;
  private final Chooser chooser;
  private final TimestampExtractor timestampExtractor;
  private final Map<TopicPartition, RecordQueue> stash = new HashMap<>();

  private final int desiredUnprocessed;

  // TODO: merge stash, consumedOffset, and newRecordBuffer into sth. like partition metadata
  private final Map<TopicPartition, Long> consumedOffsets;
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();
  private final ArrayDeque<NewRecords> newRecordBuffer = new ArrayDeque<>();

  private long streamTime = -1;
  private boolean commitRequested = false;
  private volatile int buffered = 0;

  /**
   * Creates StreamGroup
   * @param name the name of group
   * @param ingestor the instance of {@link Ingestor}
   * @param chooser the instance of {@link Chooser}
   * @param timestampExtractor the instance of {@link TimestampExtractor}
   * @param desiredUnprocessedPerPartition the target number of records kept in a queue for each topic
   */
  StreamGroup(String name,
              Ingestor ingestor,
              Chooser chooser,
              TimestampExtractor timestampExtractor,
              int desiredUnprocessedPerPartition) {
    this.name = name;
    this.ingestor = ingestor;
    this.chooser = chooser;
    this.timestampExtractor = timestampExtractor;
    this.desiredUnprocessed = desiredUnprocessedPerPartition;
    this.consumedOffsets = new HashMap<>();
  }

  public String name() {
    return name;
  }

  /**
   * Merges a stream group into this group
   */
  public void mergeStreamGroup(StreamGroup other) {
    // check these groups have the same ingestor
    if (!this.ingestor.equals(other.ingestor))
      throw new IllegalArgumentException("groups with different ingestors cannot be merged");

    // check these group have the same chooser and time extractor types
    if (!this.chooser.getClass().equals(other.chooser.getClass()))
      throw new IllegalArgumentException("groups with different type of choosers cannot be merged");

    if (!this.timestampExtractor.getClass().equals(other.timestampExtractor.getClass()))
      throw new IllegalArgumentException("groups with different type of time extractors cannot be merged");

    // add all the other's groups partitions
    for (TopicPartition partition : other.stash.keySet()) {
      this.stash.put(partition, other.stash.get(partition));
      this.consumedOffsets.put(partition, other.consumedOffsets.get(partition));
    }

    // add all the other's buffered records
    for (NewRecords records : other.newRecordBuffer) {
      this.newRecordBuffer.addLast(records);
    }
  }

  /**
   * Adds a partition and its receiver to this stream synchronizer
   * @param partition the partition
   * @param stream the instance of KStreamImpl
   */
  @SuppressWarnings("unchecked")
  public void addPartition(TopicPartition partition, KStreamSource stream) {
    synchronized (this) {
      RecordQueue recordQueue = stash.get(partition);

      if (recordQueue == null) {
        stash.put(partition, createRecordQueue(partition, stream));
      } else {
        throw new IllegalStateException("duplicate partition");
      }
    }
  }

  /**
   * Adds records
   * @param partition the partition
   * @param iterator the iterator of records
   */
  @SuppressWarnings("unchecked")
  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
    synchronized (this) {
      newRecordBuffer.addLast(new NewRecords(partition, iterator));
    }
  }

  @SuppressWarnings("unchecked")
  private void ingestNewRecords() {
    for (NewRecords newRecords : newRecordBuffer) {
      TopicPartition partition = newRecords.partition;
      Iterator<ConsumerRecord<byte[], byte[]>> iterator = newRecords.iterator;

      RecordQueue recordQueue = stash.get(partition);
      if (recordQueue != null) {
        boolean wasEmpty = recordQueue.isEmpty();

        while (iterator.hasNext()) {
          ConsumerRecord<byte[], byte[]> record = iterator.next();

          // deserialize the raw record, extract the timestamp and put into the queue
          Deserializer<?> keyDeserializer = recordQueue.stream.keyDeserializer;
          Deserializer<?> valDeserializer = recordQueue.stream.valueDeserializer;

          Object key = keyDeserializer.deserialize(record.topic(), record.key());
          Object value = valDeserializer.deserialize(record.topic(), record.value());
          ConsumerRecord deserializedRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), key, value);

          long timestamp = timestampExtractor.extract(record.topic(), key, value);
          recordQueue.add(new StampedRecord(deserializedRecord, timestamp));
          buffered++;
        }

        int queueSize = recordQueue.size();
        if (wasEmpty && queueSize > 0) chooser.add(recordQueue);

        // if we have buffered enough for this partition, pause
        if (queueSize >= this.desiredUnprocessed) {
          ingestor.pause(partition);
        }
      }
    }
    newRecordBuffer.clear();
  }

  /**
   * Returns a PunctuationScheduler
   * @param processor the processor requesting scheduler
   * @return PunctuationScheduler
   */
  public PunctuationScheduler getPunctuationScheduler(Processor<?, ?> processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
  }

  /**
   * Processes one record
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean process() {
    synchronized (this) {
      boolean readyForNextExecution = false;
      ingestNewRecords();

      RecordQueue recordQueue = chooser.next();
      if (recordQueue == null) {
        return false;
      }

      if (recordQueue.size() == 0) throw new IllegalStateException("empty record queue");

      if (recordQueue.size() == this.desiredUnprocessed) {
        ingestor.unpause(recordQueue.partition(), recordQueue.offset());
      }

      long trackedTimestamp = recordQueue.trackedTimestamp();
      StampedRecord record = recordQueue.next();

      if (streamTime < trackedTimestamp) streamTime = trackedTimestamp;

      recordQueue.stream.receive(record.key(), record.value(), record.timestamp, streamTime);
      consumedOffsets.put(recordQueue.partition(), record.offset());

      // TODO: local state flush and downstream producer flush
      // need to be done altogether with offset commit atomically
      if (commitRequested) ingestor.commit(Collections.singletonMap(
          new TopicPartition(record.topic(), record.partition()),
          record.offset()));

      if (recordQueue.size() > 0) {
        readyForNextExecution = true;
        chooser.add(recordQueue);
      }


      buffered--;

      punctuationQueue.mayPunctuate(streamTime);

      return readyForNextExecution;
    }
  }

  /**
   * Returns consumed offsets
   * @return the map of partition to consumed offset
   */
  public Map<TopicPartition, Long> consumedOffsets() {
    return this.consumedOffsets;
  }

  /**
   * Request committing the current record's offset
   */
  public void commitOffset() {
    this.commitRequested = true;
  }

  public int buffered() {
    return buffered;
  }

  public void close() {
    chooser.close();
    stash.clear();
  }

  protected RecordQueue createRecordQueue(TopicPartition partition, KStreamSource stream) {
    return new RecordQueue(partition, stream, new MinTimestampTracker<ConsumerRecord<Object, Object>>());
  }

  private static class NewRecords {
    final TopicPartition partition;
    final Iterator<ConsumerRecord<byte[], byte[]>> iterator;

    NewRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
      this.partition = partition;
      this.iterator = iterator;
    }
  }
}
