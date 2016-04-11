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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.kafka.inputformat.KafkaInputFormatIT;
import org.apache.crunch.kafka.inputformat.KafkaRecordReaderIT;
import org.apache.crunch.kafka.utils.KafkaBrokerTestHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    //org.apache.crunch.kafka
    KafkaSourceIT.class, KafkaRecordsIterableIT.class, KafkaDataIT.class,
    //org.apache.crunch.kafka.inputformat
    KafkaRecordReaderIT.class, KafkaInputFormatIT.class, KafkaUtilsIT.class,
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
    KafkaUtils.addKafkaConnectionProperties(getConsumerProperties(), kafkaConfig);
    return kafkaConfig;
  }

  public static List<String> writeData(Properties props, String topic, String batch, int loops, int numValuesPerLoop) {
    Properties producerProps = new Properties();
    producerProps.putAll(props);
    producerProps.setProperty("serializer.class", StringEncoderDecoder.class.getName());
    producerProps.setProperty("key.serializer.class", StringEncoderDecoder.class.getName());

    // Set the default compression used to be snappy
    producerProps.setProperty("compression.codec", "snappy");
    producerProps.setProperty("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(producerProps);

    Producer<String, String> producer = new Producer<>(producerConfig);
    List<String> keys = new LinkedList<>();
    try {
      for (int i = 0; i < loops; i++) {
        List<KeyedMessage<String, String>> events = new LinkedList<>();
        for (int j = 0; j < numValuesPerLoop; j++) {
          String key = "key" + batch + i + j;
          String value = "value" + batch + i + j;
          keys.add(key);
          events.add(new KeyedMessage<>(topic, key, value));
        }
        producer.send(events);
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

  public static class StringEncoderDecoder implements Encoder<String>, Decoder<String> {

    public StringEncoderDecoder() {

    }

    public StringEncoderDecoder(VerifiableProperties props) {

    }

    @Override
    public String fromBytes(byte[] bytes) {
      return new String(bytes);
    }

    @Override
    public byte[] toBytes(String value) {
      return value.getBytes();
    }
  }
}