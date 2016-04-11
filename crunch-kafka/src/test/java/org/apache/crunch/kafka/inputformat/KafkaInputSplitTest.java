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
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class KafkaInputSplitTest {

  @Rule
  public TestName testName = new TestName();

  @Test
  public void createSplit() throws IOException, InterruptedException {
    String topic = testName.getMethodName();
    int partition = 18;
    long startingOffet = 10;
    long endingOffset = 23;


    KafkaInputSplit split = new KafkaInputSplit(topic, partition, startingOffet, endingOffset);
    assertThat(split.getStartingOffset(), is(startingOffet));
    assertThat(split.getEndingOffset(), is(endingOffset));
    assertThat(split.getTopicPartition(), is(new TopicPartition(topic, partition)));
    assertThat(split.getLength(), is(endingOffset - startingOffet));
    assertThat(split.getLocations(), is(new String[0]));
  }

  @Test
  public void createSplitEarliestOffset() throws IOException, InterruptedException {
    String topic = testName.getMethodName();
    int partition = 18;
    long endingOffset = 23;

    KafkaInputSplit split = new KafkaInputSplit(topic, partition, -1L, endingOffset);
    assertThat(split.getStartingOffset(), is(-1L));
    assertThat(split.getEndingOffset(), is(endingOffset));
    assertThat(split.getTopicPartition(), is(new TopicPartition(topic, partition)));
    assertThat(split.getLength(), is(endingOffset));
    assertThat(split.getLocations(), is(new String[0]));
  }
}
