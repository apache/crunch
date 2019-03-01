/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.crunch.kafka.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRecordReaderTest {

  @Mock
  private KafkaConsumer<String, String> consumer;

  @Mock
  private TaskAttemptContext taskAttemptContext;

  private TopicPartition topicPartition;
  private long startOffset;
  private long endOffset;
  private KafkaInputSplit inputSplit;

  private ConsumerRecords<String, String> records;

  private KafkaRecordReader<String, String> reader;

  @Before
  public void before() throws IOException, InterruptedException {
    when(taskAttemptContext.getConfiguration()).thenReturn(new Configuration(false));
    startOffset = 0L;
    endOffset = 100L;
    topicPartition = new TopicPartition("topic", 0);

    inputSplit = new KafkaInputSplit(topicPartition.topic(), topicPartition.partition(), startOffset, endOffset);

    when(consumer.beginningOffsets(Collections.singleton(inputSplit.getTopicPartition()))).thenReturn(
        Collections.singletonMap(inputSplit.getTopicPartition(), 0L));

    records = new ConsumerRecords<>(Collections.singletonMap(inputSplit.getTopicPartition(),
        Collections.singletonList(new ConsumerRecord<>("topic", 0, 0, "key", "value"))));

    when(consumer.poll(anyLong())).thenReturn(records);

    reader = new KafkaRecordReaderTester();
    reader.initialize(inputSplit, taskAttemptContext);
  }

  @Test
  public void getRecords_consumerPollThrowsException_thenReturnsMessage() {
    // DisconnectException is retriable
    when(consumer.poll(anyLong())).thenThrow(new DisconnectException()).thenReturn(records);

    reader.loadRecords();
    Iterator<ConsumerRecord<String, String>> iterator = reader.getRecordIterator();
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(records.records(topicPartition).get(0)));
  }

  @Test
  public void getRecords_consumerPollEmpty_thenReturnsMessage() {
    // DisconnectException is retriable
    when(consumer.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.<TopicPartition,
        List<ConsumerRecord<String, String>>> emptyMap())).thenReturn(records);

    reader.loadRecords();
    Iterator<ConsumerRecord<String, String>> iterator = reader.getRecordIterator();
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(records.records(topicPartition).get(0)));
  }

  @Test
  public void nextKeyValue() throws IOException, InterruptedException {
    assertThat(reader.nextKeyValue(), is(true));
    assertThat(reader.getCurrentKey(), is(records.records(topicPartition).get(0)));
    assertThat(reader.getCurrentOffset(), is(0L));
  }

  @Test
  public void nextKeyValue_recordOffsetAheadOfExpected() throws IOException, InterruptedException {
    records = new ConsumerRecords<>(Collections.singletonMap(inputSplit.getTopicPartition(),
        Collections.singletonList(new ConsumerRecord<>("topic", 0, 10L, "key", "value"))));

    when(consumer.poll(anyLong())).thenReturn(records);

    assertThat(reader.nextKeyValue(), is(true));
    assertThat(reader.getCurrentKey(), is(records.records(topicPartition).get(0)));
    assertThat(reader.getCurrentOffset(), is(10L));
  }

  @Test
  public void nextKeyValue_noRecord_emptyPartition() throws IOException, InterruptedException {
    when(consumer.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.<TopicPartition,
        List<ConsumerRecord<String, String>>> emptyMap()));

    when(consumer.beginningOffsets(Collections.singletonList(topicPartition))).thenReturn(
        Collections.singletonMap(topicPartition, endOffset));

    assertThat(reader.nextKeyValue(), is(false));
  }

  @Test(expected = IOException.class)
  public void nextKeyValue_noRecord_nonEmptyPartition() throws IOException, InterruptedException {
    when(consumer.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.<TopicPartition,
        List<ConsumerRecord<String, String>>> emptyMap()));

    reader.nextKeyValue();
  }

  @Test
  public void nextKeyValue_recordIsBeyondEndOffset() throws IOException, InterruptedException {
    records = new ConsumerRecords<>(Collections.singletonMap(inputSplit.getTopicPartition(),
        Collections.singletonList(new ConsumerRecord<>("topic", 0, 100L, "key", "value"))));

    when(consumer.poll(anyLong())).thenReturn(records);

    assertThat(reader.nextKeyValue(), is(false));
  }

  @Test
  public void getEarliestOffset_noOffsetFound() {
    when(consumer.beginningOffsets(Collections.singletonList(inputSplit.getTopicPartition()))).thenReturn(
        Collections.<TopicPartition, Long> emptyMap());
    assertThat(reader.getEarliestOffset(), is(0L));
  }

  @Test
  public void getEarliestOffset() {
    when(consumer.beginningOffsets(Collections.singletonList(inputSplit.getTopicPartition()))).thenReturn(
        Collections.singletonMap(inputSplit.getTopicPartition(), 100L));
    assertThat(reader.getEarliestOffset(), is(100L));
  }

  private class KafkaRecordReaderTester extends KafkaRecordReader<String, String> {
    @Override
    protected KafkaConsumer<String, String> buildConsumer(Properties properties) {
      return consumer;
    }
  }
}