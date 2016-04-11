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

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.cluster.EndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Simple utilities for retrieving offset and Kafka information to assist in setting up and configuring a
 * {@link KafkaSource} instance.
 */
public class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  private static final String CLIENT_ID = "crunch-kafka-client";

  private static final Random RANDOM = new Random();

  /**
   * Configuration property for the number of retry attempts that will be made to Kafka.
   */
  public static final String KAFKA_RETRY_ATTEMPTS_KEY = "org.apache.crunch.kafka.retry.attempts";

  /**
   * Default number of retry attempts.
   */
  public static final int KAFKA_RETRY_ATTEMPTS_DEFAULT = 5;
  public static final String KAFKA_RETRY_ATTEMPTS_DEFAULT_STRING = Integer.toString(KAFKA_RETRY_ATTEMPTS_DEFAULT);

  /**
   * Converts the provided {@code config} into a {@link Properties} object to connect with Kafka.
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
   * @param properties the properties to add to the config.
   * @param config the configuration instance to be modified.
   * @return the config instance with the populated properties
   */
  public static Configuration addKafkaConnectionProperties(Properties properties, Configuration config) {
    for (String name : properties.stringPropertyNames()) {
      config.set(name, properties.getProperty(name));
    }
    return config;
  }

  /**
   * Returns a {@link TopicMetadataRequest} from the given topics
   *
   * @param topics an array of topics you want metadata for
   * @return a {@link TopicMetadataRequest} from the given topics
   * @throws IllegalArgumentException if topics is {@code null} or empty, or if any of the topics is null, empty or blank
   */
  private static TopicMetadataRequest getTopicMetadataRequest(String... topics) {
    if (topics == null)
      throw new IllegalArgumentException("topics cannot be null");
    if (topics.length == 0)
      throw new IllegalArgumentException("topics cannot be empty");

    for (String topic : topics)
      if (StringUtils.isBlank(topic))
        throw new IllegalArgumentException("No topic can be null, empty or blank");

    return new TopicMetadataRequest(Arrays.asList(topics));
  }

  /**
   * <p>
   * Retrieves the offset values for an array of topics at the specified time.
   * </p>
   * <p>
   * If the Kafka cluster does not have the logs for the partition at the specified time or if the topic did not exist
   * at that time this will instead return the earliest offset for that partition.
   * </p>
   *
   * @param properties the properties containing the configuration for kafka
   * @param time       the time at which we want to know what the offset values were
   * @param topics     the topics we want to know the offset values of
   * @return the offset values for an array of topics at the specified time
   * @throws IllegalArgumentException if properties is {@code null} or if topics is {@code null} or empty or if any of
   *                                  the topics are {@code null}, empty or blank, or if there is an error parsing the
   *                                  properties.
   * @throws IllegalStateException if there is an error communicating with the Kafka cluster to retrieve information.
   */
  public static Map<TopicPartition, Long> getBrokerOffsets(Properties properties, long time, String... topics) {
    if (properties == null)
      throw new IllegalArgumentException("properties cannot be null");

    final List<Broker> brokers = getBrokers(properties);
    Collections.shuffle(brokers, RANDOM);

    return getBrokerOffsets(brokers, time, topics);
  }

  // Visible for testing
  static Map<TopicPartition, Long> getBrokerOffsets(List<Broker> brokers, long time, String... topics) {
    if (topics == null)
      throw new IllegalArgumentException("topics cannot be null");
    if (topics.length == 0)
      throw new IllegalArgumentException("topics cannot be empty");

    for (String topic : topics)
      if (StringUtils.isBlank(topic))
        throw new IllegalArgumentException("No topic can be null, empty or blank");

    TopicMetadataResponse topicMetadataResponse = null;

    final TopicMetadataRequest topicMetadataRequest = getTopicMetadataRequest(topics);

    for (final Broker broker : brokers) {
      final SimpleConsumer consumer = getSimpleConsumer(broker);
      try {
        topicMetadataResponse = consumer.send(topicMetadataRequest);
        break;
      } catch (Exception err) {
        EndPoint endpoint = broker.endPoints().get(SecurityProtocol.PLAINTEXT).get();
        LOG.warn(String.format("Fetching topic metadata for topic(s) '%s' from broker '%s' failed",
            Arrays.toString(topics), endpoint.host()), err);
      } finally {
        consumer.close();
      }
    }

    if (topicMetadataResponse == null) {
      throw new IllegalStateException(String.format("Fetching topic metadata for topic(s) '%s' from broker(s) '%s' failed",
          Arrays.toString(topics), Arrays.toString(brokers.toArray())));
    }

    // From the topic metadata, build a PartitionOffsetRequestInfo for each partition of each topic. It should be noted that
    // only the leader Broker has the partition offset information[1] so save the leader Broker so we
    // can send the request to it.
    // [1] - https://cwiki.apache.org/KAFKA/a-guide-to-the-kafka-protocol.html#AGuideToTheKafkaProtocol-OffsetAPI
    Map<Broker, Map<TopicAndPartition, PartitionOffsetRequestInfo>> brokerRequests =
        new HashMap<>();

    for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
      for (PartitionMetadata partition : metadata.partitionsMetadata()) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
            new HashMap<>();

        BrokerEndPoint brokerEndPoint = partition.leader();
        Broker leader = new Broker(0, brokerEndPoint.host(), brokerEndPoint.port(), SecurityProtocol.PLAINTEXT);

        if (brokerRequests.containsKey(leader))
          requestInfo = brokerRequests.get(leader);

        requestInfo.put(new TopicAndPartition(metadata.topic(), partition.partitionId()), new PartitionOffsetRequestInfo(
            time, 1));

        brokerRequests.put(leader, requestInfo);
      }
    }

    Map<TopicPartition, Long> topicPartitionToOffset = new HashMap<>();

    // Send the offset request to the leader broker
    for (Map.Entry<Broker, Map<TopicAndPartition, PartitionOffsetRequestInfo>> brokerRequest : brokerRequests.entrySet()) {
      SimpleConsumer simpleConsumer = getSimpleConsumer(brokerRequest.getKey());

      OffsetResponse offsetResponse = null;
      try {
        OffsetRequest offsetRequest = new OffsetRequest(brokerRequest.getValue(), kafka.api.OffsetRequest.CurrentVersion(),
            CLIENT_ID);
        offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
      } finally {
        simpleConsumer.close();
      }

      Map<TopicPartition, Long> earliestOffsets = null;

      // Retrieve/parse the results
      for (Map.Entry<TopicAndPartition, PartitionOffsetRequestInfo> entry : brokerRequest.getValue().entrySet()) {
        TopicAndPartition topicAndPartition = entry.getKey();
        TopicPartition topicPartition = new TopicPartition(topicAndPartition.topic(), topicAndPartition.partition());
        long[] offsets = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        long offset;

        // The Kafka API will return no value if a time is given which there is no log that contains messages from that time
        // (i.e. before a topic existed or in a log that was rolled/cleaned)
        if (offsets.length > 0) {
          offset = offsets[0];
        } else {
          LOG.info("Kafka did not have an offset for topic/partition [{}]. Returning earliest known offset instead",
              topicAndPartition);

          // This shouldn't happen but if kafka's API did not provide us with a value and we are asking for the earliest
          // time we can't be sure what to do so quit
          if (time == kafka.api.OffsetRequest.EarliestTime())
            throw new IllegalStateException("We requested the earliest offsets for topic [" + topicAndPartition.topic()
                + "] but Kafka returned no values");

          // Load the earliest offsets for the topic if it hasn't been loaded already
          if (earliestOffsets == null)
            earliestOffsets = getBrokerOffsets(Arrays.asList(brokerRequest.getKey()),
                kafka.api.OffsetRequest.EarliestTime(), topicAndPartition.topic());

          offset = earliestOffsets.get(topicPartition);
        }

        topicPartitionToOffset.put(topicPartition, offset);
      }
    }

    return topicPartitionToOffset;
  }

  /**
   * Returns a {@link SimpleConsumer} connected to the given {@link Broker}
   */
  private static SimpleConsumer getSimpleConsumer(final Broker broker) {
    // BrokerHost, BrokerPort, timeout, buffer size, client id
    EndPoint endpoint = broker.endPoints().get(SecurityProtocol.PLAINTEXT).get();
    return new SimpleConsumer(endpoint.host(), endpoint.port(), 100000, 64 * 1024, CLIENT_ID);
  }

  /**
   * Returns a {@link Broker} list from the given {@link Properties}
   *
   * @param properties the {@link Properties} with configuration to connect to a Kafka broker
   */
  private static List<Broker> getBrokers(final Properties properties) {
    if (properties == null)
      throw new IllegalArgumentException("props cannot be null");

    String commaDelimitedBrokerList = properties.getProperty("metadata.broker.list");
    if (commaDelimitedBrokerList == null)
      throw new IllegalArgumentException("Unable to find 'metadata.broker.list' in given properties");

    // Split broker list into host/port pairs
    String[] brokerPortList = commaDelimitedBrokerList.split(",");
    if (brokerPortList.length < 1)
      throw new IllegalArgumentException("Unable to parse broker list : [" + Arrays.toString(brokerPortList) + "]");

    final List<Broker> brokers = new ArrayList<Broker>(brokerPortList.length);
    for (final String brokerHostPortString : brokerPortList) {
      // Split host/port
      String[] brokerHostPort = brokerHostPortString.split(":");
      if (brokerHostPort.length != 2)
        throw new IllegalArgumentException("Unable to parse host/port from broker string : ["
            + Arrays.toString(brokerHostPort) + "] from broker list : [" + Arrays.toString(brokerPortList) + "]");
      try {
        brokers.add(new Broker(0, brokerHostPort[0], Integer.parseInt(brokerHostPort[1]), SecurityProtocol.PLAINTEXT));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Error parsing broker port : " + brokerHostPort[1], e);
      }
    }
    return brokers;
  }

}
