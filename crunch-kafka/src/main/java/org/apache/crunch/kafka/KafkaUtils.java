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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Simple utilities for retrieving offset and Kafka information to assist in setting up and configuring a
 * {@link org.apache.crunch.kafka.record.KafkaSource} instance.
 */
public class KafkaUtils {

  /**
   * Configuration property for the number of retry attempts that will be made to Kafka.
   */
  public static final String KAFKA_RETRY_ATTEMPTS_KEY = "org.apache.crunch.kafka.retry.attempts";

  /**
   * Default number of retry attempts.
   */
  public static final int KAFKA_RETRY_ATTEMPTS_DEFAULT = 120;
  public static final String KAFKA_RETRY_ATTEMPTS_DEFAULT_STRING = Integer.toString(KAFKA_RETRY_ATTEMPTS_DEFAULT);

  /**
   * Converts the provided {@code config} into a {@link Properties} object to connect with Kafka.
   *
   * @param config the config to read properties
   * @return a properties instance populated with all of the values inside the provided {@code config}.
   */
  public static Properties getKafkaConnectionProperties(Configuration config) {
    Properties props = new Properties();
    for (Map.Entry<String, String> value : config) {
      props.setProperty(value.getKey(), value.getValue());
    }

    return props;
  }

  /**
   * Adds the {@code properties} to the provided {@code config} instance.
   *
   * @param properties the properties to add to the config.
   * @param config     the configuration instance to be modified.
   * @return the config instance with the populated properties
   */
  public static Configuration addKafkaConnectionProperties(Properties properties, Configuration config) {
    for (String name : properties.stringPropertyNames()) {
      config.set(name, properties.getProperty(name));
    }
    return config;
  }

  /**
   * Returns the {@link TopicPartition}s in a topic, returns an empty list if the topic does not exist.
   */
  public static List<TopicPartition> getTopicPartitions(Consumer<?, ?> kafkaConsumer, String topic) {
    List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

    // This conversion of null to empty list is consistent with https://issues.apache.org/jira/browse/KAFKA-2358
    if (partitionInfos == null) {
      return ImmutableList.of();
    } else {
      return partitionInfos.stream()
              .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              .collect(Collectors.toList());
    }
  }

}
