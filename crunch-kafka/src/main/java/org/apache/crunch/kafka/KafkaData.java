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
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

class KafkaData<K, V> implements ReadableData<Pair<K, V>> {

  private static final long serialVersionUID = -6582212311361579556L;

  private final Map<TopicPartition, Pair<Long, Long>> offsets;
  private final Properties props;

  public KafkaData(Properties connectionProperties,
                   Map<TopicPartition, Pair<Long, Long>> offsets) {
    this.props = connectionProperties;
    this.offsets = offsets;
  }


  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    return null;
  }

  @Override
  public void configure(Configuration conf) {
    //no-op
  }

  @Override
  public Iterable<Pair<K, V>> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
    Consumer<K, V> consumer = new KafkaConsumer<K, V>(props);
    return new KafkaRecordsIterable<>(consumer, offsets, props);
  }
}
