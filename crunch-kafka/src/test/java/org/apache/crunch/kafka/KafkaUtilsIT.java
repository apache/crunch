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

import kafka.cluster.Broker;
import org.apache.crunch.kafka.ClusterTest;
import org.apache.crunch.kafka.KafkaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;


public class KafkaUtilsIT {

  @Rule
  public TestName testName = new TestName();

  private String topic;
  private static Broker broker;

  @BeforeClass
  public static void startup() throws Exception {
    ClusterTest.startTest();

    Properties props = ClusterTest.getConsumerProperties();
    String brokerHostPorts = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

    String brokerHostPortString = brokerHostPorts.split(",")[0];
    String[] brokerHostPort = brokerHostPortString.split(":");

    String brokerHost = brokerHostPort[0];
    int brokerPort = Integer.parseInt(brokerHostPort[1]);

    broker = new Broker(0, brokerHost, brokerPort, SecurityProtocol.PLAINTEXT);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    ClusterTest.endTest();
  }

  @Before
  public void setup() throws IOException {
    topic = "topic-" + testName.getMethodName();
  }

  @Test
  public void getKafkaProperties() {
    Configuration config = new Configuration(false);
    String propertyKey = "fake.kafka.property";
    String propertyValue = testName.getMethodName();
    config.set(propertyKey, propertyValue);

    Properties props = KafkaUtils.getKafkaConnectionProperties(config);
    assertThat(props.get(propertyKey), is((Object) propertyValue));
  }

  @Test
  public void addKafkaProperties() {
    String propertyKey = "fake.kafka.property";
    String propertyValue = testName.getMethodName();

    Properties props = new Properties();
    props.setProperty(propertyKey, propertyValue);

    Configuration config = new Configuration(false);

    KafkaUtils.addKafkaConnectionProperties(props, config);
    assertThat(config.get(propertyKey), is(propertyValue));
  }


  @Test(expected = IllegalArgumentException.class)
  public void getBrokerOffsetsKafkaNullProperties() throws IOException {
    KafkaUtils.getBrokerOffsets((Properties) null, kafka.api.OffsetRequest.LatestTime(), topic);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getBrokerOffsetsKafkaNullTopics() throws IOException {
    KafkaUtils.getBrokerOffsets(ClusterTest.getConsumerProperties(), kafka.api.OffsetRequest.LatestTime(), (String[]) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getBrokerOffsetsKafkaEmptyTopics() throws IOException {
    KafkaUtils.getBrokerOffsets(ClusterTest.getConsumerProperties(), kafka.api.OffsetRequest.LatestTime());
  }

  @Test(timeout = 10000)
  public void getLatestBrokerOffsetsKafka() throws IOException, InterruptedException {
    ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 1, 4);
    while (true) {
      Map<TopicPartition, Long> offsets = KafkaUtils.getBrokerOffsets(ClusterTest.getConsumerProperties(),
          kafka.api.OffsetRequest.LatestTime(), topic);

      assertNotNull(offsets);
      assertThat(offsets.size(), is(4));
      boolean allMatch = true;
      for (int i = 0; i < 4; i++) {
        TopicPartition tp = new TopicPartition(topic, i);
        assertThat(offsets.keySet(), hasItem(tp));
        allMatch &= (offsets.get(tp) == 1L);
      }
      if (allMatch) {
        break;
      }
      Thread.sleep(100L);
    }
  }

  @Test
  public void getEarliestBrokerOffsetsKafka() throws IOException {
    ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 1, 1);

    Map<TopicPartition, Long> offsets = KafkaUtils.getBrokerOffsets(ClusterTest.getConsumerProperties(),
        kafka.api.OffsetRequest.EarliestTime(), topic);

    assertNotNull(offsets);
    //default create 4 topics
    assertThat(offsets.size(), is(4));
    for (int i = 0; i < 4; i++) {
      assertThat(offsets.keySet(), hasItem(new TopicPartition(topic, i)));
      assertThat(offsets.get(new TopicPartition(topic, i)), is(0L));
    }
  }

  @Test
  public void getBrokerOffsetsKafkaWithTimeBeforeTopicExists() throws IOException {
    ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 1, 4);

    // A time of 1L (1 ms after epoch) should be before the topic was created
    Map<TopicPartition, Long> offsets = KafkaUtils.getBrokerOffsets(ClusterTest.getConsumerProperties(), 1L, topic);

    assertNotNull(offsets);
    //default create 4 topics
    assertThat(offsets.size(), is(4));
    for (int i = 0; i < 4; i++) {
      assertThat(offsets.keySet(), hasItem(new TopicPartition(topic, i)));
      assertThat(offsets.get(new TopicPartition(topic, i)), is(0L));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void getBrokerOffsetsNoHostAvailable() throws IOException {
    Properties testProperties = ClusterTest.getConsumerProperties();
    testProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyBrokerHost1:0000,dummyBrokerHost2:0000");
    testProperties.setProperty("metadata.broker.list", "dummyBrokerHost1:0000,dummyBrokerHost2:0000");
    assertNotNull(KafkaUtils.getBrokerOffsets(testProperties, kafka.api.OffsetRequest.LatestTime(), topic));
  }

  @Test
  public void getBrokerOffsetsSomeHostsUnavailable() throws IOException {
    final Broker bad = new Broker(0, "dummyBrokerHost1", 0, SecurityProtocol.PLAINTEXT);
    assertNotNull(KafkaUtils.getBrokerOffsets(Arrays.asList(broker, bad), kafka.api.OffsetRequest.LatestTime(), topic));
    assertNotNull(KafkaUtils.getBrokerOffsets(Arrays.asList(bad, broker), kafka.api.OffsetRequest.LatestTime(), topic));
  }
}
