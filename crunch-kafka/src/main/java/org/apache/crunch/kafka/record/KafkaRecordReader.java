/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka.record;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_DEFAULT;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_KEY;
import static org.apache.crunch.kafka.KafkaUtils.getKafkaConnectionProperties;
import static org.apache.crunch.kafka.record.KafkaInputFormat.filterConnectionProperties;
import static org.apache.crunch.kafka.record.KafkaSource.CONSUMER_POLL_TIMEOUT_DEFAULT;
import static org.apache.crunch.kafka.record.KafkaSource.CONSUMER_POLL_TIMEOUT_KEY;

/**
 * A {@link RecordReader} for pulling data from Kafka.
 *
 * @param <K> the key of the records from Kafka
 * @param <V> the value of the records from Kafka
 */
public class KafkaRecordReader<K, V> extends RecordReader<ConsumerRecord<K, V>, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordReader.class);

  private Consumer<K, V> consumer;
  private ConsumerRecord<K, V> record;
  private long endingOffset;
  private Iterator<ConsumerRecord<K, V>> recordIterator;
  private long consumerPollTimeout;
  private long maxNumberOfRecords;
  private long startingOffset;
  private long currentOffset;
  private int maxNumberAttempts;
  private Properties kafkaConnectionProperties;
  private TopicPartition topicPartition;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (!(inputSplit instanceof KafkaInputSplit)) {
      throw new CrunchRuntimeException("InputSplit for RecordReader is not valid split type.");
    }

    kafkaConnectionProperties = filterConnectionProperties(getKafkaConnectionProperties(taskAttemptContext.getConfiguration()));

    consumer = buildConsumer(kafkaConnectionProperties);

    KafkaInputSplit split = (KafkaInputSplit) inputSplit;
    topicPartition = split.getTopicPartition();

    consumer.assign(Collections.singletonList(topicPartition));

    //suggested hack to gather info without gathering data
    consumer.poll(0);

    //now seek to the desired start location
    startingOffset = split.getStartingOffset();
    consumer.seek(topicPartition, startingOffset);

    currentOffset = startingOffset - 1;
    endingOffset = split.getEndingOffset();

    maxNumberOfRecords = endingOffset - startingOffset;
    if (LOG.isInfoEnabled()) {
      LOG.info("Reading data from {} between {} and {}", new Object[] { topicPartition, startingOffset, endingOffset });
    }

    Configuration config = taskAttemptContext.getConfiguration();
    consumerPollTimeout = config.getLong(CONSUMER_POLL_TIMEOUT_KEY, CONSUMER_POLL_TIMEOUT_DEFAULT);
    maxNumberAttempts = config.getInt(KAFKA_RETRY_ATTEMPTS_KEY, KAFKA_RETRY_ATTEMPTS_DEFAULT);
  }

  /**
   * Builds a new kafka consumer
   *
   * @param properties
   *      the properties to configure the consumer
   * @return a new kafka consumer
   */
  // Visible for testing
  protected KafkaConsumer<K, V> buildConsumer(Properties properties) {
    return new KafkaConsumer<>(properties);
  }

  /**
   * @return the current offset for the reader
   */
  // Visible for testing
  protected long getCurrentOffset() {
    return currentOffset;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (hasPendingData()) {
      loadRecords();
      record = recordIterator.hasNext() ? recordIterator.next() : null;
      if (record != null) {
        LOG.debug("nextKeyValue: Retrieved record with offset {}", record.offset());
        long oldOffset = currentOffset;
        currentOffset = record.offset();
        LOG.debug("Current offset will be updated to be [{}]", currentOffset);
        if (LOG.isWarnEnabled() && (currentOffset - oldOffset > 1)) {
          // The most likely scenario here is our start offset was deleted and an offset reset took place by the consumer
          LOG.warn("Possible data loss in partition {} as offset {} was larger than expected {}",
                  new Object[] { topicPartition, currentOffset, oldOffset + 1});
        }

        if (currentOffset >= endingOffset) {
          // We had pending data but read a record beyond our end offset so don't include it and stop reading
          if (LOG.isWarnEnabled())
            LOG.warn("Record offset {} is beyond our end offset {}. This could indicate data loss in partition {}",
                  new Object[] { currentOffset, endingOffset, topicPartition});
          record = null;
          return false;
        }

        return true;
      } else if (isPartitionEmpty()) {
        // If the partition is empty but we are expecting to read data we would read no records and
        // see no errors so handle that here
        if (LOG.isWarnEnabled())
          LOG.warn("The partition {} is empty though we expected to read from {} to {}. This could indicate data loss",
                  new Object[] {topicPartition, currentOffset, endingOffset});
      } else {
        // We have pending data but we are unable to fetch any records so throw an exception and stop the job
        throw new IOException("Unable to read additional data from Kafka. See logs for details. Partition " +
                topicPartition + " Current Offset: " + currentOffset + " End Offset: " + endingOffset);
      }
    }
    record = null;
    return false;
  }

  private boolean isPartitionEmpty() {
    // We don't need to fetch the ending offset as well since that is looked up when running the job. If the end offset had
    // changed while running we would have read that record instead of reading no records and calling this method
    return getEarliestOffset() == endingOffset;
  }

  @Override
  public ConsumerRecord<K, V> getCurrentKey() throws IOException, InterruptedException {
    return record;
  }

  @Override
  public Void getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    //not most accurate but gives reasonable estimate
    return ((float) (currentOffset - startingOffset + 1)) / maxNumberOfRecords;
  }

  private boolean hasPendingData() {
    //offset range is exclusive at the end which means the ending offset is one higher
    // than the actual physical last offset
    return currentOffset < endingOffset - 1;
  }

  /**
   * @return the record iterator used by the reader
   */
  // Visble for testing
  protected Iterator<ConsumerRecord<K, V>> getRecordIterator() {
    return recordIterator;
  }

  /**
   * Loads new records into the record iterator
   */
  // Visible for testing
  protected void loadRecords() {
    if ((recordIterator == null) || !recordIterator.hasNext()) {
      ConsumerRecords<K, V> records = null;
      int numTries = 0;
      boolean success = false;
      while(!success && (numTries < maxNumberAttempts)) {
        numTries++;
        try {
          records = getConsumer().poll(consumerPollTimeout);
        } catch (RetriableException re) {
          LOG.warn("Error pulling messages from Kafka", re);
        }

        // There was no error but we don't have any records. This could indicate a slowness or failure in Kafka's internal
        // client or that the partition has no more data
        if (records != null && records.isEmpty()){
          if (LOG.isWarnEnabled())
            LOG.warn("No records retrieved from partition {} with poll timeout {} but pending offsets to consume. Current "
                  + "Offset: {}, End Offset: {}", new Object[] { topicPartition, consumerPollTimeout, currentOffset,
                    endingOffset });
        } else if (records != null && !records.isEmpty()){
          success = true;
        }

        if (!success && numTries < maxNumberAttempts) {
          LOG.info("Record fetch attempt {} / {} failed, retrying", numTries, maxNumberAttempts);
        }
        else if (!success) {
          if (LOG.isWarnEnabled())
            LOG.warn("Record fetch attempt {} / {} failed. No more attempts left for partition {}",
                    new Object[] { numTries, maxNumberAttempts, topicPartition });
        }
      }

      if ((records == null) || records.isEmpty()){
        LOG.info("No records retrieved from Kafka partition {} therefore nothing to iterate over", topicPartition);
      } else{
        LOG.info("Retrieved {} records from Kafka partition {} to iterate over starting from offset {}", new Object[] {
            records.count(), topicPartition, records.iterator().next().offset()});
      }

      recordIterator = records != null ? records.iterator() : ConsumerRecords.<K, V>empty().iterator();
    }
  }

  /**
   * @return the consumer
   */
  protected Consumer<K, V> getConsumer() {
    return consumer;
  }

  /**
   * @return the earliest offset of the topic partition
   */
  protected long getEarliestOffset() {
    Map<TopicPartition, Long> brokerOffsets = consumer.beginningOffsets(
        Collections.singletonList(topicPartition));
    Long offset = brokerOffsets.get(topicPartition);
    if(offset == null){
      LOG.debug("Unable to determine earliest offset for {} so returning 0", topicPartition);
      return 0L;
    }
    LOG.debug("Earliest offset for {} is {}", topicPartition, offset);
    return offset;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("Closing the record reader.");
    if (consumer != null) {
      consumer.close();
    }
  }
}