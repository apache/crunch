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

import org.apache.crunch.Pair;
import org.apache.crunch.kafka.ClusterTest;
import org.apache.crunch.kafka.KafkaUtils;
import org.apache.crunch.kafka.utils.KafkaTestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.apache.crunch.kafka.ClusterTest.writeData;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KafkaDataIT {
  @Rule
  public TestName testName = new TestName();

  private String topic;
  private Map<TopicPartition, Long> startOffsets;
  private Map<TopicPartition, Long> stopOffsets;
  private Map<TopicPartition, Pair<Long, Long>> offsets;
  private Properties props;
  private Consumer<String, String> consumer;

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
    topic = UUID.randomUUID().toString();

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

    consumer = new KafkaConsumer<>(props);
  }

  @Test
  public void getDataIterable() throws IOException {
    int loops = 10;
    int numPerLoop = 100;
    int total = loops * numPerLoop;
    List<String> keys = writeData(props, topic, "batch", loops, numPerLoop);

    startOffsets = KafkaTestUtils.getStartOffsets(consumer, topic);
    stopOffsets = KafkaTestUtils.getStopOffsets(consumer, topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), stopOffsets.get(entry.getKey())));
    }

    Iterable<ConsumerRecord<String, String>> data = new KafkaData<String, String>(props, offsets).read(null);

    int count = 0;
    for (ConsumerRecord<String, String> record : data) {
      assertThat(keys, hasItem(record.key()));
      assertTrue(keys.remove(record.key()));
      count++;
    }

    assertThat(count, is(total));
    assertThat(keys.size(), is(0));
  }
}
