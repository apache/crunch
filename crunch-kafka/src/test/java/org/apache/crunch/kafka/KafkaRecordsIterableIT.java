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


import kafka.api.OffsetRequest;
import org.apache.crunch.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.crunch.kafka.ClusterTest.writeData;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRecordsIterableIT {

  @Mock
  private Consumer<String, String> mockedConsumer;

  @Mock
  private ConsumerRecords<String, String> records;

  @Rule
  public TestName testName = new TestName();

  private String topic;
  private Map<TopicPartition, Long> startOffsets;
  private Map<TopicPartition, Long> stopOffsets;
  private Map<TopicPartition, Pair<Long, Long>> offsets;
  private Consumer<String, String> consumer;
  private Properties props;
  private Properties consumerProps;

  @BeforeClass
  public static void init() throws Exception {
    ClusterTest.startTest();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    ClusterTest.endTest();
  }

  @Before
  public void setup() {
    topic = testName.getMethodName();

    props = ClusterTest.getConsumerProperties();

    startOffsets = new HashMap<>();
    stopOffsets = new HashMap<>();
    offsets = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      startOffsets.put(tp, 0L);
      stopOffsets.put(tp, 100L);

      offsets.put(tp, Pair.of(0L, 100L));
    }


    consumerProps = new Properties();
    consumerProps.putAll(props);
  }

  @After
  public void shutdown() {
  }


  @Test(expected = IllegalArgumentException.class)
  public void nullConsumer() {
    new KafkaRecordsIterable(null, offsets, new Properties());
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullOffsets() {
    new KafkaRecordsIterable<>(consumer, null, new Properties());
  }

  @Test(expected=IllegalArgumentException.class)
  public void emptyOffsets() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<String, String>(consumer,
        Collections.<TopicPartition, Pair<Long, Long>>emptyMap(), new Properties());
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullProperties() {
    new KafkaRecordsIterable(consumer, offsets, null);
  }

  @Test
  public void iterateOverValues() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 10;
    int numPerLoop = 100;
    int total = loops * numPerLoop;
    List<String> keys = writeData(props, topic, "batch", loops, numPerLoop);

    startOffsets = getStartOffsets(props, topic);
    stopOffsets = getStopOffsets(props, topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
    }


    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<String, String>(consumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      assertThat(keys, hasItem(event.first()));
      assertTrue(keys.remove(event.first()));
      count++;
    }

    assertThat(count, is(total));
    assertThat(keys.size(), is(0));
  }

  @Test
  public void iterateOverOneValue() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 1;
    int numPerLoop = 1;
    int total = loops * numPerLoop;
    List<String> keys = writeData(props, topic, "batch", loops, numPerLoop);

    startOffsets = getStartOffsets(props, topic);
    stopOffsets = getStopOffsets(props, topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
      System.out.println(entry.getKey()+ "start:"+entry.getValue()+":end:"+stopOffsets.get(entry.getKey()));
    }

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<String, String>(consumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      assertThat(keys, hasItem(event.first()));
      assertTrue(keys.remove(event.first()));
      count++;
    }

    assertThat(count, is(total));
    assertThat(keys.size(), is(0));
  }

  @Test
  public void iterateOverNothing() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 10;
    int numPerLoop = 100;
    writeData(props, topic, "batch", loops, numPerLoop);

    //set the start offsets equal to the stop so won't iterate over anything
    startOffsets = getStartOffsets(props, topic);
    stopOffsets = getStartOffsets(props, topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
    }

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(consumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      count++;
    }

    assertThat(count, is(0));
  }

  @Test
  public void iterateOverPartial() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 10;
    int numPerLoop = 100;
    int numPerPartition = 50;

    writeData(props, topic, "batch", loops, numPerLoop);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    //set the start offsets equal to the stop so won't iterate over anything
    startOffsets = getStartOffsets(props, topic);
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), entry.getValue() + numPerPartition));
    }

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(consumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      count++;
    }

    assertThat(count, is(startOffsets.size() * numPerPartition));
  }

  @Test
  public void dontIteratePastStop() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 10;
    int numPerLoop = 100;

    List<String> keys = writeData(props, topic, "batch1", loops, numPerLoop);

    //set the start offsets equal to the stop so won't iterate over anything
    startOffsets = getStartOffsets(props, topic);
    stopOffsets = getStopOffsets(props, topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
    }

    List<String> secondKeys = writeData(props, topic, "batch2", loops, numPerLoop);

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(consumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      assertThat(keys, hasItem(event.first()));
      assertTrue(keys.remove(event.first()));
      assertThat(secondKeys, not(hasItem(event.first())));
      count++;
    }

    assertThat(count, is(loops * numPerLoop));
    assertThat(keys.size(), is(0));
  }

  @Test
  public void iterateSkipInitialValues() {
    consumer = new KafkaConsumer<String, String>(consumerProps, new ClusterTest.StringSerDe(), new ClusterTest.StringSerDe());
    int loops = 10;
    int numPerLoop = 100;

    List<String> keys = writeData(props, topic, "batch1", loops, numPerLoop);

    //set the start offsets equal to the stop so won't iterate over anything
    startOffsets = getStopOffsets(props, topic);

    List<String> secondKeys = writeData(props, topic, "batch2", loops, numPerLoop);

    stopOffsets = getStopOffsets(props, topic);


    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
    }


    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<String, String>(consumer, offsets,
        new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      assertThat(secondKeys, hasItem(event.first()));
      assertTrue(secondKeys.remove(event.first()));
      assertThat(keys, not(hasItem(event.first())));
      count++;
    }

    assertThat(count, is(loops * numPerLoop));
    assertThat(secondKeys.size(), is(0));
  }

  @Test
  public void iterateValuesWithExceptions() {
    List<ConsumerRecord<String, String>> returnedRecords = new LinkedList<>();

    for(int i = 0; i < 25; i++){
      returnedRecords.add(new ConsumerRecord<String, String>(topic, 0, i, "key", null));
      returnedRecords.add(new ConsumerRecord<String, String>(topic, 1, i, "key", null));
      returnedRecords.add(new ConsumerRecord<String, String>(topic, 2, i, "key", null));
      returnedRecords.add(new ConsumerRecord<String, String>(topic, 3, i, "key", null));
    }

    offsets = new HashMap<>();
    offsets.put(new TopicPartition(topic, 0), Pair.of(0L, 25L));
    offsets.put(new TopicPartition(topic, 1), Pair.of(0L, 25L));
    offsets.put(new TopicPartition(topic, 2), Pair.of(0L, 25L));
    offsets.put(new TopicPartition(topic, 3), Pair.of(0L, 25L));

    when(records.isEmpty()).thenReturn(false);
    when(records.iterator()).thenReturn(returnedRecords.iterator());
    when(mockedConsumer.poll(Matchers.anyLong()))
        //request for the first poll
        .thenReturn(null)
        //fail twice
        .thenThrow(new TimeoutException("fail1"))
        .thenThrow(new TimeoutException("fail2"))
        //request that will give data
        .thenReturn(records)
        // shows to stop retrieving data
        .thenReturn(null);

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(mockedConsumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      count++;
    }

    //should have gotten one value per topicpartition
    assertThat(count, is(returnedRecords.size()));
  }

  @Test
  public void iterateValuesAfterStopOffsets() {
    List<ConsumerRecord<String, String>> returnedRecords = new LinkedList<>();
    for (Map.Entry<TopicPartition, Long> entry : stopOffsets.entrySet()) {
      returnedRecords.add(new ConsumerRecord<String, String>(entry.getKey().topic(),
          entry.getKey().partition(), entry.getValue() + 1, "key", null));
    }

    when(records.isEmpty()).thenReturn(false);
    when(records.iterator()).thenReturn(returnedRecords.iterator());
    when(mockedConsumer.poll(Matchers.anyLong())).thenReturn(records).thenReturn(records).thenReturn(null);

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(mockedConsumer, offsets, new Properties());

    int count = 0;
    for (Pair<String, String> event : data) {
      count++;
    }

    assertThat(count, is(0));

  }

  @Test(expected = RetriableException.class)
  public void iterateRetriableExceptionMaxExceeded() {
    List<ConsumerRecord<String, String>> returnedRecords = new LinkedList<>();
    for (Map.Entry<TopicPartition, Long> entry : stopOffsets.entrySet()) {
      returnedRecords.add(new ConsumerRecord<String, String>(entry.getKey().topic(),
          entry.getKey().partition(), entry.getValue() + 1, "key", null));
    }

    when(records.isEmpty()).thenReturn(false);
    when(records.iterator()).thenReturn(returnedRecords.iterator());
    when(mockedConsumer.poll(Matchers.anyLong()))
        //for the fill poll call
        .thenReturn(null)
        //retry 5 times then fail
        .thenThrow(new TimeoutException("fail1"))
        .thenThrow(new TimeoutException("fail2"))
        .thenThrow(new TimeoutException("fail3"))
        .thenThrow(new TimeoutException("fail4"))
        .thenThrow(new TimeoutException("fail5"))
        .thenThrow(new TimeoutException("fail6"));

    Iterable<Pair<String, String>> data = new KafkaRecordsIterable<>(mockedConsumer, offsets, new Properties());

    data.iterator().next();
  }

  private static Map<TopicPartition, Long> getStopOffsets(Properties props, String topic) {
    return KafkaUtils.getBrokerOffsets(props, OffsetRequest.LatestTime(), topic);
  }

  private static Map<TopicPartition, Long> getStartOffsets(Properties props, String topic) {
    return KafkaUtils.getBrokerOffsets(props, OffsetRequest.EarliestTime(), topic);
  }
}
