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
package org.apache.crunch.kafka.inputformat;

import kafka.api.OffsetRequest;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.kafka.KafkaUtils;
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

import static org.apache.crunch.kafka.KafkaSource.CONSUMER_POLL_TIMEOUT_DEFAULT;
import static org.apache.crunch.kafka.KafkaSource.CONSUMER_POLL_TIMEOUT_KEY;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_EMPTY_RETRY_ATTEMPTS_KEY;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_DEFAULT;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_KEY;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_EMPTY_ATTEMPTS_DEFAULT;
import static org.apache.crunch.kafka.KafkaUtils.getKafkaConnectionProperties;
import static org.apache.crunch.kafka.inputformat.KafkaInputFormat.filterConnectionProperties;

/**
 * A {@link RecordReader} for pulling data from Kafka.
 * @param <K> the key of the records from Kafka
 * @param <V> the value of the records from Kafka
 */
public class KafkaRecordReader<K, V> extends RecordReader<K, V> {

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
  private Properties connectionProperties;
  private TopicPartition topicPartition;
  private int concurrentEmptyResponses;
  private int maxConcurrentEmptyResponses;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if(!(inputSplit instanceof KafkaInputSplit)){
      throw new CrunchRuntimeException("InputSplit for RecordReader is not valid split type.");
    }
    Properties kafkaConnectionProperties = filterConnectionProperties(
            getKafkaConnectionProperties(taskAttemptContext.getConfiguration()));

    consumer = new KafkaConsumer<>(kafkaConnectionProperties);
    KafkaInputSplit split = (KafkaInputSplit) inputSplit;
    TopicPartition topicPartition = split.getTopicPartition();

    consumer.assign(Collections.singletonList(topicPartition));

    //suggested hack to gather info without gathering data
    consumer.poll(0);
    //now seek to the desired start location
    startingOffset = split.getStartingOffset();
    consumer.seek(topicPartition,startingOffset);

    currentOffset = startingOffset - 1;
    endingOffset = split.getEndingOffset();

    maxNumberOfRecords = endingOffset - startingOffset;
    if(LOG.isInfoEnabled()) {
      LOG.info("Reading data from {} between {} and {}", new Object[]{topicPartition, startingOffset, endingOffset});
    }

    Configuration config = taskAttemptContext.getConfiguration();
    consumerPollTimeout = config.getLong(CONSUMER_POLL_TIMEOUT_KEY, CONSUMER_POLL_TIMEOUT_DEFAULT);
    maxNumberAttempts = config.getInt(KAFKA_RETRY_ATTEMPTS_KEY, KAFKA_RETRY_ATTEMPTS_DEFAULT);
    maxConcurrentEmptyResponses = config.getInt(KAFKA_EMPTY_RETRY_ATTEMPTS_KEY, KAFKA_RETRY_EMPTY_ATTEMPTS_DEFAULT);
    concurrentEmptyResponses = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(hasPendingData()) {
      recordIterator = getRecords();
      record = recordIterator.hasNext() ? recordIterator.next() : null;
      if (record != null) {
        LOG.debug("nextKeyValue: Retrieved record with offset {}", record.offset());
        long oldOffset = currentOffset;
        currentOffset = record.offset();
        LOG.debug("Current offset will be updated to be [{}]", currentOffset);
        if (LOG.isWarnEnabled() && (currentOffset - oldOffset > 1)) {
          LOG.warn("Offset increment was larger than expected value of one, old {} new {}", oldOffset, currentOffset);
        }
        return true;
      } else {
        LOG.warn("nextKeyValue: Retrieved null record last offset was {} and ending offset is {}", currentOffset, endingOffset);
      }
    }
    record = null;
    return false;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return record == null ? null : record.key();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return record == null ? null : record.value();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    //not most accurate but gives reasonable estimate
    return ((float) (currentOffset - startingOffset +1)) / maxNumberOfRecords;
  }

  private boolean hasPendingData(){
    //offset range is exclusive at the end which means the ending offset is one higher
    // than the actual physical last offset

    boolean hasPending = currentOffset < endingOffset-1;

    if(concurrentEmptyResponses > maxConcurrentEmptyResponses){
      long earliest = getEarliestOffset();
      if(earliest == endingOffset){
        LOG.warn("Possible data loss for {} as earliest {} is equal to {} and greater than expected current {}.",
          new Object[]{topicPartition, earliest, endingOffset, currentOffset});
        return false;
      }
    }

    return hasPending;
  }

  private Iterator<ConsumerRecord<K, V>> getRecords() {
    if ((recordIterator == null) || !recordIterator.hasNext()) {
      ConsumerRecords<K, V> records = null;
      int numTries = 0;
      boolean success = false;
      while(!success && (numTries < maxNumberAttempts)) {
        try {
          records = getConsumer().poll(consumerPollTimeout);
        } catch (RetriableException re) {
          numTries++;
          if (numTries < maxNumberAttempts) {
            LOG.warn("Error pulling messages from Kafka. Retrying with attempt {}", numTries+1, re);
          } else {
            LOG.error("Error pulling messages from Kafka. Exceeded maximum number of attempts {}", maxNumberAttempts, re);
            throw re;
          }
        }
        if(((records == null) || records.isEmpty()) && hasPendingData()){
          concurrentEmptyResponses++;
          LOG.warn("No records retrieved but pending offsets to consume therefore polling again. Attempt {}/{}",
            concurrentEmptyResponses, maxConcurrentEmptyResponses);
        }else{
          success = true;
        }
      }
      concurrentEmptyResponses = 0;

      if((records == null) || records.isEmpty()){
        LOG.info("No records retrieved from Kafka therefore nothing to iterate over.");
      }else{
        LOG.info("Retrieved records from Kafka to iterate over.");
      }
      return records != null ? records.iterator() : ConsumerRecords.<K, V>empty().iterator();
    }
    return recordIterator;
  }

  protected Consumer<K,V> getConsumer(){
    return consumer;
  }

  protected long getEarliestOffset(){
    Map<TopicPartition, Long> brokerOffsets = KafkaUtils
      .getBrokerOffsets(connectionProperties, OffsetRequest.EarliestTime(), topicPartition.topic());
    Long offset = brokerOffsets.get(topicPartition);
    if(offset == null){
      LOG.debug("Unable to determine earliest offset for {} so returning earliest {}", topicPartition,
        OffsetRequest.EarliestTime());
      return OffsetRequest.EarliestTime();
    }
    LOG.debug("Earliest offset for {} is {}", topicPartition, offset);
    return offset;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("Closing the record reader.");
    if(consumer != null) {
      consumer.close();
    }
  }
}