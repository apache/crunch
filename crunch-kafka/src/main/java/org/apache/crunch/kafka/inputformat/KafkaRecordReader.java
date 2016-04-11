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

import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_DEFAULT;
import static org.apache.crunch.kafka.KafkaUtils.KAFKA_RETRY_ATTEMPTS_KEY;
import static org.apache.crunch.kafka.KafkaUtils.getKafkaConnectionProperties;
import static org.apache.crunch.kafka.KafkaSource.CONSUMER_POLL_TIMEOUT_DEFAULT;
import static org.apache.crunch.kafka.KafkaSource.CONSUMER_POLL_TIMEOUT_KEY;

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
  private int maxNumberAttempts;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    consumer = new KafkaConsumer<>(getKafkaConnectionProperties(taskAttemptContext.getConfiguration()));
    KafkaInputSplit split = (KafkaInputSplit) inputSplit;
    TopicPartition topicPartition = split.getTopicPartition();
    consumer.assign(Collections.singletonList(topicPartition));
    //suggested hack to gather info without gathering data
    consumer.poll(0);
    //now seek to the desired start location
    startingOffset = split.getStartingOffset();
    consumer.seek(topicPartition,startingOffset);

    endingOffset = split.getEndingOffset();

    maxNumberOfRecords = endingOffset - split.getStartingOffset();
    if(LOG.isInfoEnabled()) {
      LOG.info("Reading data from {} between {} and {}", new Object[]{topicPartition, startingOffset, endingOffset});
    }

    Configuration config = taskAttemptContext.getConfiguration();
    consumerPollTimeout = config.getLong(CONSUMER_POLL_TIMEOUT_KEY, CONSUMER_POLL_TIMEOUT_DEFAULT);
    maxNumberAttempts = config.getInt(KAFKA_RETRY_ATTEMPTS_KEY, KAFKA_RETRY_ATTEMPTS_DEFAULT);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    recordIterator = getRecords();
    record = recordIterator.hasNext() ? recordIterator.next() : null;
    if(LOG.isDebugEnabled()){
      if(record != null) {
        LOG.debug("nextKeyValue: Retrieved record with offset {}", record.offset());
      }else{
        LOG.debug("nextKeyValue: Retrieved null record");
      }
    }
    return record != null && record.offset() < endingOffset;
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
    return record == null ? 0.0f : ((float) (record.offset()- startingOffset)) / maxNumberOfRecords;
  }

  private Iterator<ConsumerRecord<K, V>> getRecords() {
    if (recordIterator == null || !recordIterator.hasNext()) {
      ConsumerRecords<K, V> records = null;
      int numTries = 0;
      boolean notSuccess = false;
      while(!notSuccess && numTries < maxNumberAttempts) {
        try {
          records = consumer.poll(consumerPollTimeout);
          notSuccess = true;
        } catch (RetriableException re) {
          numTries++;
          if (numTries < maxNumberAttempts) {
            LOG.warn("Error pulling messages from Kafka. Retrying with attempt {}", numTries, re);
          } else {
            LOG.error("Error pulling messages from Kafka. Exceeded maximum number of attempts {}", maxNumberAttempts, re);
            throw re;
          }
        }
      }

      if(LOG.isDebugEnabled() && records != null){
        LOG.debug("No records retrieved from Kafka therefore nothing to iterate over.");
      }else{
        LOG.debug("Retrieved records from Kafka to iterate over.");
      }
      return records != null ? records.iterator() : ConsumerRecords.<K, V>empty().iterator();
    }
    return recordIterator;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("Closing the record reader.");
    if(consumer != null) {
      consumer.close();
    }
  }
}