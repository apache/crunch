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
import org.apache.crunch.Pair;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.kafka.ClusterTest;
import org.apache.crunch.kafka.KafkaSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.crunch.kafka.KafkaUtils.getBrokerOffsets;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaInputFormatIT {

  @Rule
  public TestName testName = new TestName();

  @Mock
  private TaskAttemptContext taskContext;

  @Mock
  private FormatBundle bundle;
  private Properties consumerProps;
  private Configuration config;
  private String topic;

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startTest();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    ClusterTest.endTest();
  }

  @Before
  public void setupTest() {
    topic = testName.getMethodName();
    consumerProps = ClusterTest.getConsumerProperties();

    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSource.BytesDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSource.BytesDeserializer.class.getName());

    config = ClusterTest.getConsumerConfig();

    config.set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSource.BytesDeserializer.class.getName());
    config.set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSource.BytesDeserializer.class.getName());
  }

  @Test
  public void getSplitsFromFormat() throws IOException, InterruptedException {
    List<String> keys = ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 10, 10);
    Map<TopicPartition, Long> startOffsets = getBrokerOffsets(consumerProps, OffsetRequest.EarliestTime(), topic);
    Map<TopicPartition, Long> endOffsets = getBrokerOffsets(consumerProps, OffsetRequest.LatestTime(), topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      Long endingOffset = endOffsets.get(entry.getKey());
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), endingOffset));
    }

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    KafkaInputFormat inputFormat = new KafkaInputFormat();
    inputFormat.setConf(config);
    List<InputSplit> splits = inputFormat.getSplits(null);

    assertThat(splits.size(), is(offsets.size()));

    for (InputSplit split : splits) {
      KafkaInputSplit inputSplit = (KafkaInputSplit) split;
      Pair<Long, Long> startEnd = offsets.get(inputSplit.getTopicPartition());
      assertThat(inputSplit.getStartingOffset(), is(startEnd.first()));
      assertThat(inputSplit.getEndingOffset(), is(startEnd.second()));
    }
  }

  @Test
  public void getSplitsSameStartEnd() throws IOException, InterruptedException {

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for(int i = 0; i < 10; i++) {
      offsets.put(new TopicPartition(topic, i), Pair.of((long)i, (long)i));
    }

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    KafkaInputFormat inputFormat = new KafkaInputFormat();
    inputFormat.setConf(config);
    List<InputSplit> splits = inputFormat.getSplits(null);

    assertThat(splits.size(), is(0));
  }

  @Test
  public void getSplitsCreateReaders() throws IOException, InterruptedException {
    List<String> keys = ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 10, 10);
    Map<TopicPartition, Long> startOffsets = getBrokerOffsets(consumerProps, OffsetRequest.EarliestTime(), topic);
    Map<TopicPartition, Long> endOffsets = getBrokerOffsets(consumerProps, OffsetRequest.LatestTime(), topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      Long endingOffset = endOffsets.get(entry.getKey());
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), endingOffset));
    }

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    KafkaInputFormat inputFormat = new KafkaInputFormat();
    inputFormat.setConf(config);
    List<InputSplit> splits = inputFormat.getSplits(null);

    assertThat(splits.size(), is(offsets.size()));

    for (InputSplit split : splits) {
      KafkaInputSplit inputSplit = (KafkaInputSplit) split;
      Pair<Long, Long> startEnd = offsets.get(inputSplit.getTopicPartition());
      assertThat(inputSplit.getStartingOffset(), is(startEnd.first()));
      assertThat(inputSplit.getEndingOffset(), is(startEnd.second()));
    }

    //create readers and consume the data
    when(taskContext.getConfiguration()).thenReturn(config);
    Set<String> keysRead = new HashSet<>();
    //read all data from all splits
    for (InputSplit split : splits) {
      KafkaInputSplit inputSplit = (KafkaInputSplit) split;
      long start = inputSplit.getStartingOffset();
      long end = inputSplit.getEndingOffset();

      RecordReader<BytesWritable, BytesWritable> recordReader = inputFormat.createRecordReader(split, taskContext);
      recordReader.initialize(split, taskContext);

      int numRecordsFound = 0;
      while (recordReader.nextKeyValue()) {
        keysRead.add(new String(recordReader.getCurrentKey().getBytes()));
        assertThat(keys, hasItem(new String(recordReader.getCurrentKey().getBytes())));
        assertThat(recordReader.getCurrentValue(), is(notNullValue()));
        numRecordsFound++;
      }
      recordReader.close();

      //assert that it encountered a partitions worth of data
      assertThat(((long) numRecordsFound), is(end - start));
    }

    //validate the same number of unique keys was read as were written.
    assertThat(keysRead.size(), is(keys.size()));
  }

  @Test
  public void writeOffsetsToFormatBundle() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    String topic = testName.getMethodName();
    int numPartitions = 10;
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tAndP = new TopicPartition(topic, i);
      offsets.put(tAndP, Pair.of((long) i, i * 10L));
    }

    KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

    //number of Partitions * 2 for start and end + 1 for the topic
    verify(bundle, times((numPartitions * 2) + 1)).set(keyCaptor.capture(), valueCaptor.capture());

    List<String> keyValues = keyCaptor.getAllValues();
    List<String> valueValues = valueCaptor.getAllValues();

    String partitionKey = KafkaInputFormat.generateTopicPartitionsKey(topic);
    assertThat(keyValues, hasItem(partitionKey));

    String partitions = valueValues.get(keyValues.indexOf(partitionKey));
    List<String> parts = Arrays.asList(partitions.split(","));

    for (int i = 0; i < numPartitions; i++) {
      assertThat(keyValues, hasItem(KafkaInputFormat.generateTopicPartitionsKey(topic)));
      String startKey = KafkaInputFormat.generatePartitionStartKey(topic, i);
      String endKey = KafkaInputFormat.generatePartitionEndKey(topic, i);
      assertThat(keyValues, hasItem(startKey));
      assertThat(keyValues, hasItem(endKey));
      assertThat(valueValues.get(keyValues.indexOf(startKey)), is(Long.toString(i)));
      assertThat(valueValues.get(keyValues.indexOf(endKey)), is(Long.toString(i * 10L)));
      assertThat(parts, hasItem(Long.toString(i)));
    }
  }

  @Test
  public void writeOffsetsToFormatBundleSpecialCharacters() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    String topic = "partitions." + testName.getMethodName();
    int numPartitions = 10;
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tAndP = new TopicPartition(topic, i);
      offsets.put(tAndP, Pair.of((long) i, i * 10L));
    }

    KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

    //number of Partitions * 2 for start and end + 1 for the topic
    verify(bundle, times((numPartitions * 2) + 1)).set(keyCaptor.capture(), valueCaptor.capture());

    List<String> keyValues = keyCaptor.getAllValues();
    List<String> valueValues = valueCaptor.getAllValues();

    String partitionKey = KafkaInputFormat.generateTopicPartitionsKey(topic);
    assertThat(keyValues, hasItem(partitionKey));

    String partitions = valueValues.get(keyValues.indexOf(partitionKey));
    List<String> parts = Arrays.asList(partitions.split(","));

    for (int i = 0; i < numPartitions; i++) {
      assertThat(keyValues, hasItem(KafkaInputFormat.generateTopicPartitionsKey(topic)));
      String startKey = KafkaInputFormat.generatePartitionStartKey(topic, i);
      String endKey = KafkaInputFormat.generatePartitionEndKey(topic, i);
      assertThat(keyValues, hasItem(startKey));
      assertThat(keyValues, hasItem(endKey));
      assertThat(valueValues.get(keyValues.indexOf(startKey)), is(Long.toString(i)));
      assertThat(valueValues.get(keyValues.indexOf(endKey)), is(Long.toString(i * 10L)));
      assertThat(parts, hasItem(Long.toString(i)));
    }
  }

  @Test
  public void writeOffsetsToFormatBundleMultipleTopics() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    Set<String> topics = new HashSet<>();

    int numPartitions = 10;
    int numTopics = 10;
    for (int j = 0; j < numTopics; j++) {
      String topic = testName.getMethodName() + j;
      topics.add(topic);
      for (int i = 0; i < numPartitions; i++) {
        TopicPartition tAndP = new TopicPartition(topic, i);
        offsets.put(tAndP, Pair.of((long) i, i * 10L));
      }
    }

    KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

    //number of Partitions * 2 for start and end + num of topics
    verify(bundle, times((numTopics * numPartitions * 2) + numTopics)).set(keyCaptor.capture(), valueCaptor.capture());

    List<String> keyValues = keyCaptor.getAllValues();
    List<String> valueValues = valueCaptor.getAllValues();

    for (String topic : topics) {

      String partitionKey = KafkaInputFormat.generateTopicPartitionsKey(topic);
      assertThat(keyValues, hasItem(partitionKey));

      String partitions = valueValues.get(keyValues.indexOf(partitionKey));
      List<String> parts = Arrays.asList(partitions.split(","));

      for (int i = 0; i < numPartitions; i++) {
        assertThat(keyValues, hasItem(KafkaInputFormat.generateTopicPartitionsKey(topic)));
        String startKey = KafkaInputFormat.generatePartitionStartKey(topic, i);
        String endKey = KafkaInputFormat.generatePartitionEndKey(topic, i);
        assertThat(keyValues, hasItem(startKey));
        assertThat(keyValues, hasItem(endKey));
        assertThat(valueValues.get(keyValues.indexOf(startKey)), is(Long.toString(i)));
        assertThat(valueValues.get(keyValues.indexOf(endKey)), is(Long.toString(i * 10L)));
        assertThat(parts, hasItem(Long.toString(i)));
      }
    }
  }

  @Test
  public void getOffsetsFromConfig() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    Set<String> topics = new HashSet<>();

    int numPartitions = 10;
    int numTopics = 10;
    for (int j = 0; j < numTopics; j++) {
      String topic = testName.getMethodName() + ".partitions" + j;
      topics.add(topic);
      for (int i = 0; i < numPartitions; i++) {
        TopicPartition tAndP = new TopicPartition(topic, i);
        offsets.put(tAndP, Pair.of((long) i, i * 10L));
      }
    }

    Configuration config = new Configuration(false);

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    Map<TopicPartition, Pair<Long, Long>> returnedOffsets = KafkaInputFormat.getOffsets(config);

    assertThat(returnedOffsets.size(), is(returnedOffsets.size()));
    for (Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()) {
      Pair<Long, Long> valuePair = returnedOffsets.get(entry.getKey());
      assertThat(valuePair, is(entry.getValue()));
    }
  }

  @Test(expected=IllegalStateException.class)
  public void getOffsetsFromConfigMissingStart() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    Set<String> topics = new HashSet<>();

    int numPartitions = 10;
    int numTopics = 10;
    for (int j = 0; j < numTopics; j++) {
      String topic = testName.getMethodName() + ".partitions" + j;
      topics.add(topic);
      for (int i = 0; i < numPartitions; i++) {
        TopicPartition tAndP = new TopicPartition(topic, i);
        offsets.put(tAndP, Pair.of((long) i, i * 10L));
      }
    }

    Configuration config = new Configuration(false);

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    config.unset("org.apache.crunch.kafka.offsets.topic."+topics.iterator().next()+".partitions.0.start");

    Map<TopicPartition, Pair<Long, Long>> returnedOffsets = KafkaInputFormat.getOffsets(config);
  }

  @Test(expected=IllegalStateException.class)
  public void getOffsetsFromConfigMissingEnd() {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    Set<String> topics = new HashSet<>();

    int numPartitions = 10;
    int numTopics = 10;
    for (int j = 0; j < numTopics; j++) {
      String topic = testName.getMethodName() + ".partitions" + j;
      topics.add(topic);
      for (int i = 0; i < numPartitions; i++) {
        TopicPartition tAndP = new TopicPartition(topic, i);
        offsets.put(tAndP, Pair.of((long) i, i * 10L));
      }
    }

    Configuration config = new Configuration(false);

    KafkaInputFormat.writeOffsetsToConfiguration(offsets, config);

    config.unset("org.apache.crunch.kafka.offsets.topic."+topics.iterator().next()+".partitions.0.end");

    Map<TopicPartition, Pair<Long, Long>> returnedOffsets = KafkaInputFormat.getOffsets(config);
  }

}
