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
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

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

public class KafkaSourceIT {

  @Rule
  public TemporaryPath path = new TemporaryPath();

  @Rule
  public TestName testName = new TestName();

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
    config = ClusterTest.getConsumerConfig();
  }

  @Test
  public void sourceReadData() {
    List<String> keys = ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 10, 10);
    Map<TopicPartition, Long> startOffsets = getBrokerOffsets(consumerProps, OffsetRequest.EarliestTime(), topic);
    Map<TopicPartition, Long> endOffsets = getBrokerOffsets(consumerProps, OffsetRequest.LatestTime(), topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      Long endingOffset = endOffsets.get(entry.getKey());
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), endingOffset));
    }

    Configuration config = ClusterTest.getConf();

    Pipeline pipeline = new MRPipeline(KafkaSourceIT.class, config);
    pipeline.enableDebug();

    TableSource<BytesWritable, BytesWritable> kafkaSource = new KafkaSource(consumerProps, offsets);

    PTable<BytesWritable, BytesWritable> read = pipeline.read(kafkaSource);

    Set<String> keysRead = new HashSet<>();
    int numRecordsFound = 0;
    for (Pair<BytesWritable, BytesWritable> values : read.materialize()) {
      assertThat(keys, hasItem(new String(values.first().getBytes())));
      numRecordsFound++;
      keysRead.add(new String(values.first().getBytes()));
    }

    assertThat(numRecordsFound, is(keys.size()));
    assertThat(keysRead.size(), is(keys.size()));

    pipeline.done();
  }


  @Test
  public void sourceReadDataThroughPipeline() {
    List<String> keys = ClusterTest.writeData(ClusterTest.getProducerProperties(), topic, "batch", 10, 10);
    Map<TopicPartition, Long> startOffsets = getBrokerOffsets(consumerProps, OffsetRequest.EarliestTime(), topic);
    Map<TopicPartition, Long> endOffsets = getBrokerOffsets(consumerProps, OffsetRequest.LatestTime(), topic);

    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
      Long endingOffset = endOffsets.get(entry.getKey());
      offsets.put(entry.getKey(), Pair.of(entry.getValue(), endingOffset));
    }

    Configuration config = ClusterTest.getConf();

    Pipeline pipeline = new MRPipeline(KafkaSourceIT.class, config);
    pipeline.enableDebug();

    TableSource<BytesWritable, BytesWritable> kafkaSource = new KafkaSource(consumerProps, offsets);

    PTable<BytesWritable, BytesWritable> read = pipeline.read(kafkaSource);
    Path out = path.getPath("out");
    read.parallelDo(new SimpleConvertFn(), Avros.strings()).write(To.textFile(out));

    pipeline.run();

    PCollection<String> persistedKeys = pipeline.read(From.textFile(out));

    Set<String> keysRead = new HashSet<>();
    int numRecordsFound = 0;
    for (String value : persistedKeys.materialize()) {
      assertThat(keys, hasItem(value));
      numRecordsFound++;
      keysRead.add(value);
    }

    assertThat(numRecordsFound, is(keys.size()));
    assertThat(keysRead.size(), is(keys.size()));

    pipeline.done();
  }


  private static class SimpleConvertFn extends MapFn<Pair<BytesWritable, BytesWritable>, String> {
    @Override
    public String map(Pair<BytesWritable, BytesWritable> input) {
      return new String(input.first().getBytes());
    }
  }
}
