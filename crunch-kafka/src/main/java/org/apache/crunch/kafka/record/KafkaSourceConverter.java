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

import org.apache.crunch.types.Converter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * {@link Converter} for {@link KafkaSource}
 */
public class KafkaSourceConverter implements
    Converter<ConsumerRecord<BytesWritable, BytesWritable>, Void, ConsumerRecord<BytesWritable, BytesWritable>, Iterable<ConsumerRecord<BytesWritable, BytesWritable>>> {

  private static final long serialVersionUID = 5270341393169043945L;

  @Override
  public ConsumerRecord<BytesWritable, BytesWritable> convertInput(ConsumerRecord<BytesWritable, BytesWritable> record,
      Void aVoid) {
    return record;
  }

  @Override
  public Iterable<ConsumerRecord<BytesWritable, BytesWritable>> convertIterableInput(
      ConsumerRecord<BytesWritable, BytesWritable> bytesWritableBytesWritableConsumerRecord, Iterable<Void> iterable) {
    throw new UnsupportedOperationException("Should not be possible");
  }

  @Override
  public ConsumerRecord<BytesWritable, BytesWritable> outputKey(ConsumerRecord<BytesWritable, BytesWritable> record) {
    return record;
  }

  @Override
  public Void outputValue(ConsumerRecord<BytesWritable, BytesWritable> record) {
    // No value, we just use the record as the key.
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<ConsumerRecord<BytesWritable, BytesWritable>> getKeyClass() {
    return (Class<ConsumerRecord<BytesWritable, BytesWritable>>) (Object) ConsumerRecord.class;
  }

  @Override
  public Class<Void> getValueClass() {
    return Void.class;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return false;
  }
}
