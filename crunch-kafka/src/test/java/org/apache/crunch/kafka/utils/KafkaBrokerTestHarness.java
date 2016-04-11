/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crunch.kafka.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.commons.io.FileUtils;
import scala.Option;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static scala.collection.JavaConversions.asJavaIterable;

/**
 * A test harness that brings up some number of Kafka broker nodes.
 * <p>
 * Adapted from the {@code kafka.integration.KafkaServerTestHarness} class.
 * </p>
 */
public class KafkaBrokerTestHarness extends ZookeeperTestHarness {

  /**
   * Producer send acknowledgment timeout in milliseconds.
   */
  public static final String KAFKA_PRODUCER_ACK_TIMEOUT_MILLIS = "request.timeout.ms";

  /**
   * Producer send retry maximum count.
   */
  public static final String KAFKA_PRODUCER_RETRY_MAX = "message.send.max.retries";

  /**
   * Producer send retry backoff interval in milliseconds.
   */
  public static final String KAFKA_PRODUCER_RETRY_INTERVAL_MILLIS = "retry.backoff.ms";

  /**
   * Comma-delimited Kafka Zookeeper quorum list.
   */
  public static final String KAFKA_ZOOKEEPERS = "zookeeper.connect";

  /**
   * Comma-delimited list of Kafka brokers, for producer bootstrapping purposes.
   */
  public static final String KAFKA_BROKERS = "metadata.broker.list";

  /**
   * Default number of brokers in the Kafka cluster.
   */
  public static final int DEFAULT_BROKERS = 1;

  /**
   * Default number of partitions per Kafka topic.
   */
  public static final int PARTITIONS_PER_TOPIC = 4;

  private List<KafkaConfig> brokerConfigs;
  private List<KafkaServer> brokers;
  private File clientConfig;
  private boolean setUp;
  private boolean tornDown;

  /**
   * Creates a new Kafka broker test harness using the {@link #DEFAULT_BROKERS default} number of brokers.
   */
  public KafkaBrokerTestHarness() {
    this(DEFAULT_BROKERS, KafkaTestUtils.getPorts(1)[0]);
  }

  /**
   * Creates a new Kafka broker test harness using the {@link #DEFAULT_BROKERS default} number of brokers and the supplied
   * {@link Properties} which will be applied to the brokers.
   *
   * @param properties
   *            the additional {@link Properties} supplied to the brokers
   * @throws IllegalArgumentException
   *             if {@code properties} is {@code null}
   */
  public KafkaBrokerTestHarness(Properties properties) {
    this(DEFAULT_BROKERS, KafkaTestUtils.getPorts(1)[0], properties);
  }

  /**
   * Creates a new Kafka broker test harness using the given number of brokers and Zookeeper port.
   *
   * @param brokers Number of Kafka brokers to start up.
   * @param zookeeperPort The port number to use for Zookeeper client connections.
   *
   * @throws IllegalArgumentException if {@code brokers} is less than 1.
   */
  public KafkaBrokerTestHarness(int brokers, int zookeeperPort) {
    this(getBrokerConfig(brokers, zookeeperPort), zookeeperPort);
  }

  /**
   * Creates a new Kafka broker test harness using the given number of brokers and Zookeeper port.
   *
   * @param brokers
   *            Number of Kafka brokers to start up.
   * @param zookeeperPort
   *            The port number to use for Zookeeper client connections.
   * @param properties
   *            the additional {@link Properties} supplied to the brokers
   *
   * @throws IllegalArgumentException
   *             if {@code brokers} is less than 1 or if {@code baseProperties} is {@code null}
   */
  public KafkaBrokerTestHarness(int brokers, int zookeeperPort, Properties properties) {
    this(getBrokerConfig(brokers, zookeeperPort, properties), zookeeperPort);
  }

  /**
   * Creates a new Kafka broker test harness using the given broker configuration properties and Zookeeper port.
   *
   * @param brokerConfigs List of Kafka broker configurations.
   * @param zookeeperPort The port number to use for Zookeeper client connections.
   *
   * @throws IllegalArgumentException if {@code brokerConfigs} is {@code null} or empty.
   */
  public KafkaBrokerTestHarness(List<KafkaConfig> brokerConfigs, int zookeeperPort) {
    super(zookeeperPort);
    if (brokerConfigs == null || brokerConfigs.isEmpty()) {
      throw new IllegalArgumentException("Must supply at least one broker configuration.");
    }
    this.brokerConfigs = brokerConfigs;
    this.brokers = null;
    this.setUp = false;
    this.tornDown = false;
  }

  /**
   * Start up the Kafka broker cluster.
   *
   * @throws IOException if an error occurs during Kafka broker startup.
   * @throws IllegalStateException if the Kafka broker cluster has already been {@link #setUp() setup}.
   */
  @Override
  public void setUp() throws IOException {
    if (setUp) {
      throw new IllegalStateException("Already setup, cannot setup again");
    }
    setUp = true;

    // Start up zookeeper.
    super.setUp();

    brokers = new ArrayList<KafkaServer>(brokerConfigs.size());
    for (KafkaConfig config : brokerConfigs) {
      brokers.add(startBroker(config));
    }

    // Write out Kafka client config to a temp file.
    clientConfig = new File(KafkaTestUtils.getTempDir(), "kafka-config.xml");
    FileWriter writer = new FileWriter(clientConfig);
    writer.append("<configuration>");
    for (String prop : Arrays.asList(KAFKA_BROKERS, KAFKA_ZOOKEEPERS)) {
      writer.append("<property>");
      writer.append("<name>").append(prop).append("</name>");
      writer.append("<value>").append(getProps().getProperty(prop)).append("</value>");
      writer.append("</property>");
    }
    writer.append("</configuration>");
    writer.close();
  }

  /**
   * Shutdown the Kafka broker cluster. Attempting to {@link #setUp()} a cluster again after calling this method is not allowed;
   * a new {@code KafkaBrokerTestHarness} must be created instead.
   *
   * @throws IllegalStateException if the Kafka broker cluster has already been {@link #tearDown() torn down} or has not been
   *      {@link #setUp()}.
   */
  @Override
  public void tearDown() throws IOException {
    if (!setUp) {
      throw new IllegalStateException("Not set up, cannot tear down");
    }
    if (tornDown) {
      throw new IllegalStateException("Already torn down, cannot tear down again");
    }
    tornDown = true;

    for (KafkaServer broker : brokers) {
      broker.shutdown();
    }

    for (KafkaServer broker : brokers) {
      for (String logDir : asJavaIterable(broker.config().logDirs())) {
        FileUtils.deleteDirectory(new File(logDir));
      }
    }

    // Shutdown zookeeper
    super.tearDown();
  }

  /**
   * Returns properties for a Kafka producer.
   *
   * @return Producer properties.
   */
  public Properties getProducerProps() {
    StringBuilder brokers = new StringBuilder();
    for (int i = 0; i < brokerConfigs.size(); ++i) {
      KafkaConfig config = brokerConfigs.get(i);
      brokers.append((i > 0) ? "," : "").append(config.hostName()).append(":").append(config.port());
    }

    Properties props = new Properties();
    props.setProperty(KAFKA_BROKERS, brokers.toString());
    props.setProperty(KAFKA_PRODUCER_ACK_TIMEOUT_MILLIS, "10000");

    // These two properties below are increased from their defaults to help with the case that auto.create.topics.enable is
    // disabled and a test tries to create a topic and immediately write to it
    props.setProperty(KAFKA_PRODUCER_RETRY_INTERVAL_MILLIS, Integer.toString(500));
    props.setProperty(KAFKA_PRODUCER_RETRY_MAX, Integer.toString(10));

    return props;
  }

  /**
   * Returns properties for a Kafka consumer.
   *
   * @return Consumer properties.
   */
  public Properties getConsumerProps() {
    Properties props = new Properties();
    props.setProperty(KAFKA_ZOOKEEPERS, zookeeperConnect);
    return props;
  }

  /**
   * Returns properties for either a Kafka producer or consumer.
   *
   * @return Combined producer and consumer properties.
   */
  public Properties getProps() {
    // Combine producer and consumer properties.
    Properties props = getProducerProps();
    props.putAll(getConsumerProps());
    return props;
  }

  /**
   * Returns configuration properties for each Kafka broker in the cluster.
   *
   * @return Broker properties.
   */
  public List<Properties> getBrokerProps() {
    List<Properties> props = new ArrayList<Properties>(brokers.size());
    for (KafkaServer broker : brokers) {
      Properties prop = new Properties();
      prop.putAll(broker.config().props());
      props.add(prop);
    }
    return props;
  }

  /**
   * Creates a collection of Kafka Broker configurations based on the number of brokers and zookeeper.
   * @param brokers the number of brokers to create configuration for.
   * @param zookeeperPort the zookeeper port for the brokers to connect to.
   * @return configuration for a collection of brokers.
   * @throws IllegalArgumentException if {@code brokers} is less than 1
   */
  public static List<KafkaConfig> getBrokerConfig(int brokers, int zookeeperPort) {
    return getBrokerConfig(brokers, zookeeperPort, new Properties());
  }

  /**
   * Creates a collection of Kafka Broker configurations based on the number of brokers and zookeeper.
   * @param brokers the number of brokers to create configuration for.
   * @param zookeeperPort the zookeeper port for the brokers to connect to.
   * @param baseProperties basic properties that should be applied for each broker config.  These properties will be
   *                       honored in favor of any default properties.
   * @return configuration for a collection of brokers.
   * @throws IllegalArgumentException if {@code brokers} is less than 1 or {@code baseProperties} is {@code null}.
   */
  public static List<KafkaConfig> getBrokerConfig(int brokers, int zookeeperPort, Properties baseProperties) {
    if (brokers < 1) {
      throw new IllegalArgumentException("Invalid broker count: " + brokers);
    }
    if (baseProperties == null) {
      throw new IllegalArgumentException("The 'baseProperties' cannot be 'null'.");
    }

    int ports[] = KafkaTestUtils.getPorts(brokers);

    List<KafkaConfig> configs = new ArrayList<KafkaConfig>(brokers);
    for (int i = 0; i < brokers; ++i) {
      Properties props = new Properties();
      props.setProperty(KAFKA_ZOOKEEPERS, "localhost:" + zookeeperPort);
      props.setProperty("broker.id", String.valueOf(i + 1));
      props.setProperty("host.name", "localhost");
      props.setProperty("port", String.valueOf(ports[i]));
      props.setProperty("log.dir", KafkaTestUtils.getTempDir().getAbsolutePath());
      props.setProperty("log.flush.interval.messages", String.valueOf(1));
      props.setProperty("num.partitions", String.valueOf(PARTITIONS_PER_TOPIC));
      props.setProperty("default.replication.factor", String.valueOf(brokers));
      props.setProperty("auto.create.topics.enable", Boolean.FALSE.toString());

      props.putAll(baseProperties);

      configs.add(new KafkaConfig(props));
    }
    return configs;
  }

  /**
   * Returns location of Kafka client configuration file containing broker and zookeeper connection properties.
   * <p>
   * This file can be loaded using the {@code -conf} command option to easily achieve Kafka connectivity.
   * </p>
   *
   * @return Kafka client configuration file path
   */
  public String getClientConfigPath() {
    return clientConfig.getAbsolutePath();
  }

  private static KafkaServer startBroker(KafkaConfig config) {
    KafkaServer server = new KafkaServer(config, new SystemTime(), Option.<String>empty());
    server.startup();
    return server;
  }

  private static class SystemTime implements Time {
    @Override
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
      return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }
}
