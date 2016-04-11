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
package org.apache.crunch.kafka;

import org.apache.crunch.Pair;
import org.apache.crunch.kafka.inputformat.KafkaRecordReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;


class KafkaRecordsIterable<K, V> implements Iterable<Pair<K, V>> {

  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordsIterable.class);

  /**
   * The Kafka consumer responsible for retrieving messages.
   */
  private final Consumer<K, V> consumer;

  /**
   * The starting positions of the iterable for the topic.
   */
  private final Map<TopicPartition, Pair<Long, Long>> offsets;

  /**
   * Tracks if the iterable is empty.
   */
  private final boolean isEmpty;

  /**
   * The poll time between each request to Kafka
   */
  private final long scanPollTime;

  private final int maxRetryAttempts;

  /**
   * Creates the iterable that will pull values for a collection of topics using the provided {@code consumer} between
   * the {@code startOffsets} and {@code stopOffsets}.
   * @param consumer The consumer for pulling the data from Kafka.  The consumer will be closed automatically once all
   *                 of the records have been consumed.
   * @param offsets offsets for pulling data
   * @param properties properties for tweaking the behavior of the iterable.
   * @throws IllegalArgumentException if any of the arguments are {@code null} or empty.
   */
  public KafkaRecordsIterable(Consumer<K, V> consumer, Map<TopicPartition, Pair<Long, Long>> offsets,
                              Properties properties) {
    if (consumer == null) {
      throw new IllegalArgumentException("The 'consumer' cannot be 'null'.");
    }
    this.consumer = consumer;

    if (properties == null) {
      throw new IllegalArgumentException("The 'properties' cannot be 'null'.");
    }

    String retryString = properties.getProperty(KafkaUtils.KAFKA_RETRY_ATTEMPTS_KEY,
        KafkaUtils.KAFKA_RETRY_ATTEMPTS_DEFAULT_STRING);
    maxRetryAttempts = Integer.parseInt(retryString);

    if (offsets == null || offsets.isEmpty()) {
      throw new IllegalArgumentException("The 'offsets' cannot 'null' or empty.");
    }

    //filter out any topics and partitions that do not have offset ranges that will produce data.
    Map<TopicPartition, Pair<Long, Long>> filteredOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()) {
      Pair<Long, Long> value = entry.getValue();
      //if start is less than one less than stop then there is data to be had
      if(value.first() < value.second()){
        filteredOffsets.put(entry.getKey(), value);
      }else{
        LOG.debug("Removing offsets for {} because start is not less than the end offset.", entry.getKey());
      }
    }

    //check to make sure that based on the offsets there is data to retrieve, otherwise false.
    //there will be data if the start offsets are less than stop offsets
    isEmpty = filteredOffsets.isEmpty();
    if (isEmpty) {
      LOG.warn("Iterable for Kafka for is empty because offsets are empty.");
    }

    //assign this
    this.offsets = filteredOffsets;

    scanPollTime = Long.parseLong(properties.getProperty(KafkaSource.CONSUMER_POLL_TIMEOUT_KEY,
        Long.toString(KafkaSource.CONSUMER_POLL_TIMEOUT_DEFAULT)));
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    if (isEmpty) {
      LOG.debug("Returning empty iterator since offsets align.");
      return Collections.emptyIterator();
    }
    //Assign consumer to all of the partitions
    LOG.debug("Assigning topics and partitions and seeking to start offsets.");

    consumer.assign(new LinkedList<>(offsets.keySet()));
    //hack so maybe look at removing this
    consumer.poll(0);
    for (Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()) {
      consumer.seek(entry.getKey(), entry.getValue().first());
    }

    return new RecordsIterator<K, V>(consumer, offsets, scanPollTime, maxRetryAttempts);
  }

  private static class RecordsIterator<K, V> implements Iterator<Pair<K, V>> {

    private final Consumer<K, V> consumer;
    private final Map<TopicPartition, Pair<Long, Long>> offsets;
    private final long pollTime;
    private final int maxNumAttempts;
    private ConsumerRecords<K, V> records;
    private Iterator<ConsumerRecord<K, V>> currentIterator;
    private final Set<TopicPartition> remainingPartitions;

    private Pair<K, V> next;

    public RecordsIterator(Consumer<K, V> consumer,
                           Map<TopicPartition, Pair<Long, Long>> offsets, long pollTime, int maxNumRetries) {
      this.consumer = consumer;
      remainingPartitions = new HashSet<>(offsets.keySet());
      this.offsets = offsets;
      this.pollTime = pollTime;
      this.maxNumAttempts = maxNumRetries;
    }

    @Override
    public boolean hasNext() {
      if (next != null)
        return true;

      //if partitions to consume then pull next value
      if (remainingPartitions.size() > 0) {
        next = getNext();
      }

      return next != null;
    }

    @Override
    public Pair<K, V> next() {
      if (next == null) {
        next = getNext();
      }

      if (next != null) {
        Pair<K, V> returnedNext = next;
        //prime for next call
        next = getNext();
        //return the current next
        return returnedNext;
      } else {
        throw new NoSuchElementException("No more elements.");
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove is not supported.");
    }

    /**
     * Gets the current iterator.
     *
     * @return the current iterator or {@code null} if there are no more values to consume.
     */
    private Iterator<ConsumerRecord<K, V>> getIterator() {
      if (!remainingPartitions.isEmpty()) {
        if (currentIterator != null && currentIterator.hasNext()) {
          return currentIterator;
        }
        LOG.debug("Retrieving next set of records.");
        int numTries = 0;
        boolean notSuccess = false;
        while(!notSuccess && numTries < maxNumAttempts) {
          try {
            records = consumer.poll(pollTime);
            notSuccess = true;
          }catch(RetriableException re){
            numTries++;
            if(numTries < maxNumAttempts) {
              LOG.warn("Error pulling messages from Kafka. Retrying with attempt {}", numTries, re);
            }else{
              LOG.error("Error pulling messages from Kafka. Exceeded maximum number of attempts {}", maxNumAttempts, re);
              throw re;
            }
          }
        }
        if (records == null || records.isEmpty()) {
          LOG.debug("Retrieved empty records.");
          currentIterator = null;
          return null;
        }
        currentIterator = records.iterator();
        return currentIterator;
      }

      LOG.debug("No more partitions to consume therefore not retrieving any more records.");
      return null;
    }

    /**
     * Internal method for retrieving the next value to retrieve.
     *
     * @return {@code null} if there are no more values to retrieve otherwise the next event.
     */
    private Pair<K, V> getNext() {
      while (!remainingPartitions.isEmpty()) {
        Iterator<ConsumerRecord<K, V>> iterator = getIterator();

        while (iterator != null && iterator.hasNext()) {
          ConsumerRecord<K, V> record = iterator.next();
          TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
          long offset = record.offset();

          if (withinRange(topicPartition, offset)) {
            LOG.debug("Retrieving value for {} with offset {}.", topicPartition, offset);
            return Pair.of(record.key(), record.value());
          }
          LOG.debug("Value for {} with offset {} is outside of range skipping.", topicPartition, offset);
        }
      }

      LOG.debug("Closing the consumer because there are no more remaining partitions.");
      consumer.close();

      LOG.debug("Consumed data from all partitions.");
      return null;

    }

    /**
     * Checks whether the value for {@code topicPartition} with an {@code offset} is within scan range.  If
     * the value is not then {@code false} is returned otherwise {@code true}.
     *
     * @param topicPartion The partition for the offset
     * @param offset the offset in the partition
     * @return {@code true} if the value is within the expected consumption range, otherwise {@code false}.
     */
    private boolean withinRange(TopicPartition topicPartion, long offset) {
      long endOffset = offsets.get(topicPartion).second();
      //end offsets are one higher than the last written value.
      boolean emit = offset < endOffset;
      if (offset >= endOffset - 1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed consuming partition {} with offset {} and ending offset {}.",
              new Object[]{topicPartion, offset, endOffset});
        }
        remainingPartitions.remove(topicPartion);
        consumer.pause(topicPartion);
      }
      LOG.debug("Value for partition {} and offset {} is within range.", topicPartion, offset);
      return emit;
    }
  }
}

