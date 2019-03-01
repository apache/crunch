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

import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class KafkaInputFormatTest {

  @Test
  public void generateConnectionPropertyKeyTest() {
    String propertyName = "some.property";
    String actual = KafkaInputFormat.generateConnectionPropertyKey(propertyName);
    String expected = "org.apache.crunch.kafka.connection.properties.some.property";
    assertEquals(expected, actual);
  }

  @Test
  public void getConnectionPropertyFromKeyTest() {
    String prefixedConnectionProperty = "org.apache.crunch.kafka.connection.properties.some.property";
    String actual = KafkaInputFormat.getConnectionPropertyFromKey(prefixedConnectionProperty);
    String expected = "some.property";
    assertEquals(expected, actual);
  }

  @Test
  public void writeConnectionPropertiesToBundleTest() {
    FormatBundle<KafkaInputFormat> actual = FormatBundle.forInput(KafkaInputFormat.class);
    Properties connectionProperties = new Properties();
    connectionProperties.put("key1", "value1");
    connectionProperties.put("key2", "value2");
    KafkaInputFormat.writeConnectionPropertiesToBundle(connectionProperties, actual);

    FormatBundle<KafkaInputFormat> expected = FormatBundle.forInput(KafkaInputFormat.class);
    expected.set("org.apache.crunch.kafka.connection.properties.key1", "value1");
    expected.set("org.apache.crunch.kafka.connection.properties.key2", "value2");

    assertEquals(expected, actual);
  }

  @Test
  public void filterConnectionPropertiesTest() {
    Properties props = new Properties();
    props.put("org.apache.crunch.kafka.connection.properties.key1", "value1");
    props.put("org.apache.crunch.kafka.connection.properties.key2", "value2");
    props.put("org_apache_crunch_kafka_connection_properties.key3", "value3");
    props.put("org.apache.crunch.another.prefix.properties.key4", "value4");

    Properties actual = KafkaInputFormat.filterConnectionProperties(props);
    Properties expected = new Properties();
    expected.put("key1", "value1");
    expected.put("key2", "value2");

    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getSplitsInvalidMaxRecords() throws IOException, InterruptedException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    Configuration conf = new Configuration(false);
    conf.setLong(KafkaInputFormat.KAFKA_MAX_RECORDS_PER_SPLIT, 0L);
    kafkaInputFormat.setConf(conf);
    kafkaInputFormat.getSplits(mock(JobContext.class));
  }

  @Test
  public void getSplitsConfiguredMaxRecords() throws IOException, InterruptedException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    Configuration conf = new Configuration(false);
    conf.setLong(KafkaInputFormat.KAFKA_MAX_RECORDS_PER_SPLIT, 2L);

    conf.set("org.apache.crunch.kafka.offsets.topic.abc.partitions", "0,1");
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.start", 300L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.end", 1000L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.1.start", 30L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.1.end", 100L);

    conf.set("org.apache.crunch.kafka.offsets.topic.xyz.partitions", "0");
    conf.setLong("org.apache.crunch.kafka.offsets.topic.xyz.partitions.0.start", 3L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.xyz.partitions.0.end", 10L);

    kafkaInputFormat.setConf(conf);
    List<InputSplit> splits = kafkaInputFormat.getSplits(mock(JobContext.class));
    assertThat(splits.size(), is((700/2 + 700%2) + (70/2 + 70%2) + (7/2 + 7%2)));
  }

  @Test
  public void getSplitsDefaultMaxRecords() throws IOException, InterruptedException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    Configuration conf = new Configuration(false);

    conf.set("org.apache.crunch.kafka.offsets.topic.abc.partitions", "0");
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.start", 0L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.end", 5234567L);

    kafkaInputFormat.setConf(conf);
    List<InputSplit> splits = kafkaInputFormat.getSplits(mock(JobContext.class));
    assertThat(splits.size(), is(2));
  }

  @Test
  public void getSplitsNoRecords() throws IOException, InterruptedException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    Configuration conf = new Configuration(false);

    conf.set("org.apache.crunch.kafka.offsets.topic.abc.partitions", "0");
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.start", 5L);
    conf.setLong("org.apache.crunch.kafka.offsets.topic.abc.partitions.0.end", 5L);

    kafkaInputFormat.setConf(conf);
    List<InputSplit> splits = kafkaInputFormat.getSplits(mock(JobContext.class));
    assertThat(splits.size(), is(0));
  }
}