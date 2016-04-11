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


import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.run.CrunchMapper;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.kafka.inputformat.KafkaInputFormat;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A Crunch Source that will retrieve events from Kafka given start and end offsets.  The source is not designed to
 * process unbounded data but instead to retrieve data between a specified range.
 * <p>
 *
 * The values retrieved from Kafka are returned as raw bytes inside of a {@link BytesWritable}.  If callers
 * need specific parsing logic based on the topic then consumers are encouraged to use multiple Kafka Sources
 * for each topic and use special {@link DoFn} to parse the payload.
 */
public class KafkaSource
    implements TableSource<BytesWritable, BytesWritable>, ReadableSource<Pair<BytesWritable, BytesWritable>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private final FormatBundle inputBundle;
  private final Properties props;
  private final Map<TopicPartition, Pair<Long, Long>> offsets;

  /**
   * The consistent PType describing all of the data being retrieved from Kafka as a BytesWritable.
   */
  private static PTableType<BytesWritable, BytesWritable> KAFKA_SOURCE_TYPE =
      Writables.tableOf(Writables.writables(BytesWritable.class), Writables.writables(BytesWritable.class));

  /**
   * Constant to indicate how long the reader waits before timing out when retrieving data from Kafka.
   */
  public static final String CONSUMER_POLL_TIMEOUT_KEY = "org.apache.crunch.kafka.consumer.poll.timeout";

  /**
   * Default timeout value for {@link #CONSUMER_POLL_TIMEOUT_KEY} of 1 second.
   */
  public static final long CONSUMER_POLL_TIMEOUT_DEFAULT = 1000L;


  /**
   * Constructs a Kafka source that will read data from the Kafka cluster identified by the {@code kafkaConnectionProperties}
   * and from the specific topics and partitions identified in the {@code offsets}
   * @param kafkaConnectionProperties The connection properties for reading from Kafka.  These properties will be honored
   *                                  with the exception of the {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
   *                                  {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
   * @param offsets A map of {@link TopicPartition} to a pair of start and end offsets respectively.  The start and end offsets
   *                are evaluated at [start, end) where the ending offset is excluded.  Each TopicPartition must have a
   *                non-null pair describing its offsets.  The start offset should be less than the end offset.  If the values
   *                are equal or start is greater than the end then that partition will be skipped.
   */
  public KafkaSource(Properties kafkaConnectionProperties, Map<TopicPartition, Pair<Long, Long>> offsets) {
    this.props = copyAndSetProperties(kafkaConnectionProperties);

    inputBundle = createFormatBundle(props, offsets);

    this.offsets = Collections.unmodifiableMap(new HashMap<>(offsets));
  }

  @Override
  public Source<Pair<BytesWritable, BytesWritable>> inputConf(String key, String value) {
    inputBundle.set(key, value);
    return this;
  }

  @Override
  public PType<Pair<BytesWritable, BytesWritable>> getType() {
    return KAFKA_SOURCE_TYPE;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return KAFKA_SOURCE_TYPE.getConverter();
  }

  @Override
  public PTableType<BytesWritable, BytesWritable> getTableType() {
    return KAFKA_SOURCE_TYPE;
  }

  @Override
  public long getSize(Configuration configuration) {
    // TODO something smarter here.
    return 1000L * 1000L * 1000L;
  }

  @Override
  public String toString() {
    return "KafkaSource("+props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)+")";
  }

  @Override
  public long getLastModifiedAt(Configuration configuration) {
    LOG.warn("Cannot determine last modified time for source: {}", toString());
    return -1;
  }

  private static <K, V> FormatBundle createFormatBundle(Properties kafkaConnectionProperties,
                                                        Map<TopicPartition, Pair<Long, Long>> offsets) {

    FormatBundle<KafkaInputFormat> bundle = FormatBundle.forInput(KafkaInputFormat.class);

    KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

    for (String name : kafkaConnectionProperties.stringPropertyNames()) {
      bundle.set(name, kafkaConnectionProperties.getProperty(name));
    }

    return bundle;
  }

  private static <K, V> Properties copyAndSetProperties(Properties kakfaConnectionProperties) {
    Properties props = new Properties();
    props.putAll(kakfaConnectionProperties);

    //Setting the key/value deserializer to ensure proper translation from Kafka to PType format.
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

    return props;
  }


  @Override
  public Iterable<Pair<BytesWritable, BytesWritable>> read(Configuration conf) throws IOException {
    // consumer will get closed when the iterable is fully consumed.
    // skip using the inputformat/splits since this will be read in a single JVM and don't need the complexity
    // of parallelism when reading.
    Consumer<BytesWritable, BytesWritable> consumer = new KafkaConsumer<>(props);
    return new KafkaRecordsIterable<>(consumer, offsets, props);
  }


  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration conf = job.getConfiguration();
    //an id of -1 indicates that this is the only input so just use it directly
    if (inputId == -1) {
      job.setMapperClass(CrunchMapper.class);
      job.setInputFormatClass(inputBundle.getFormatClass());
      inputBundle.configure(conf);
    } else {
      //there are multiple inputs for this mapper so add it as a CrunchInputs and need a fake path just to
      //make it play well with other file based inputs.
      Path dummy = new Path("/kafka/" + inputId);
      CrunchInputs.addInputPath(job, dummy, inputBundle, inputId);
    }
  }

  @Override
  public ReadableData<Pair<BytesWritable, BytesWritable>> asReadable() {
    // skip using the inputformat/splits since this will be read in a single JVM and don't need the complexity
    // of parallelism when reading.
    return new KafkaData<>(props, offsets);
  }


  /**
   * Basic {@link Deserializer} which simply wraps the payload as a {@link BytesWritable}.
   */
  public static class BytesDeserializer implements Deserializer<BytesWritable> {

    @Override
    public void configure(Map<String, ?> configProperties, boolean isKey) {
      //no-op
    }

    @Override
    public BytesWritable deserialize(String topic, byte[] valueBytes) {
      return new BytesWritable(valueBytes);
    }

    @Override
    public void close() {
      //no-op
    }
  }
}