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

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.kafka.record.KafkaInputFormat;
import org.apache.crunch.kafka.utils.KafkaBrokerTestHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // org.apache.crunch.kafka.record
        org.apache.crunch.kafka.record.KafkaSourceIT.class, org.apache.crunch.kafka.record.KafkaRecordsIterableIT.class,
        org.apache.crunch.kafka.record.KafkaDataIT.class
})
public class ClusterTest {


  private static TemporaryFolder folder = new TemporaryFolder();
  private static KafkaBrokerTestHarness kafka;
  private static boolean runAsSuite = false;
  private static Configuration conf;
  private static FileSystem fs;

  @BeforeClass
  public static void startSuite() throws Exception {
    runAsSuite = true;
    startKafka();
    setupFileSystem();
  }

  @AfterClass
  public static void endSuite() throws Exception {
    stopKafka();
  }

  public static void startTest() throws Exception {
    if (!runAsSuite) {
      startKafka();
      setupFileSystem();
    }
  }

  public static void endTest() throws Exception {
    if (!runAsSuite) {
      stopKafka();
    }
  }

  private static void stopKafka() throws IOException {
    kafka.tearDown();
  }

  private static void startKafka() throws IOException {
    Properties props = new Properties();
    props.setProperty("auto.create.topics.enable", Boolean.TRUE.toString());

    kafka = new KafkaBrokerTestHarness(props);
    kafka.setUp();
  }

  private static void setupFileSystem() throws IOException {
    folder.create();

    conf = new Configuration();
    conf.set(RuntimeParameters.TMP_DIR, folder.getRoot().getAbsolutePath());
    // Run Map/Reduce tests in process.
    conf.set("mapreduce.jobtracker.address", "local");
  }

  public static Configuration getConf() {
    // Clone the configuration so it doesn't get modified for other tests.
    return new Configuration(conf);
  }

  public static Properties getConsumerProperties() {
    Properties props = new Properties();
    props.putAll(kafka.getProps());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerDe.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerDe.class.getName());
    //set this because still needed by some APIs.
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("metadata.broker.list"));
    props.setProperty("enable.auto.commit", Boolean.toString(false));

    //when set this causes some problems with initializing the consumer.
    props.remove(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    return props;
  }

  public static Properties getProducerProperties() {
    Properties props = new Properties();
    props.putAll(kafka.getProps());
    //set this because still needed by some APIs.
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("metadata.broker.list"));
    return props;
  }

  public static Configuration getConsumerConfig() {
    Configuration kafkaConfig = new Configuration(conf);
    KafkaUtils.addKafkaConnectionProperties(KafkaInputFormat.tagExistingKafkaConnectionProperties(
            getConsumerProperties()), kafkaConfig);
    return kafkaConfig;
  }

  public static List<String> writeData(Properties props, String topic, String batch, int loops, int numValuesPerLoop) {
    Properties producerProps = new Properties();
    producerProps.putAll(props);
    producerProps.setProperty("value.serializer", StringSerDe.class.getName());
    producerProps.setProperty("key.serializer", StringSerDe.class.getName());

    // Set the default compression used to be snappy
    producerProps.setProperty("compression.codec", "snappy");
    producerProps.setProperty("request.required.acks", "1");

    Producer<String, String> producer = new KafkaProducer<>(producerProps);
    List<String> keys = new LinkedList<>();
    try {
      for (int i = 0; i < loops; i++) {
        for (int j = 0; j < numValuesPerLoop; j++) {
          String key = "key" + batch + i + j;
          String value = "value" + batch + i + j;
          keys.add(key);
          producer.send(new ProducerRecord<>(topic, key, value));
        }
      }
    } finally {
      producer.close();
    }
    return keys;
  }


  public static class StringSerDe implements Serializer<String>, Deserializer<String> {

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, String value) {
      return value.getBytes();
    }

    @Override
    public String deserialize(String topic, byte[] bytes) {
      return new String(bytes);
    }

    @Override
    public void close() {

    }
  }

}