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

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ConsumerRecordHelperTest {

  @Test (expected = IllegalArgumentException.class)
  public void serialize_nullRecord() throws IOException {
    ConsumerRecordHelper.serialize(null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void deserialize_nullRecord() throws IOException {
    ConsumerRecordHelper.deserialize(null);
  }

  @Test
  public void serializeDeserialize() throws IOException {
    ConsumerRecord<BytesWritable, BytesWritable> record = new ConsumerRecord<>("topic",
        1, 2, 3L, TimestampType.CREATE_TIME, 4L, 5,
        6, new BytesWritable("key".getBytes()), new BytesWritable("value".getBytes()));

    ConsumerRecord<BytesWritable, BytesWritable> newRecord = ConsumerRecordHelper.deserialize(
        ConsumerRecordHelper.serialize(record));

    assertRecordsAreEqual(record, newRecord);
  }

  @Test
  public void serializeDeserialize_nullKeyValue() throws IOException {
    ConsumerRecord<BytesWritable, BytesWritable> record = new ConsumerRecord<>("topic",
        1, 2, 3L, TimestampType.CREATE_TIME, 4L, 5,
        6, null, null);

    ConsumerRecord<BytesWritable, BytesWritable> newRecord = ConsumerRecordHelper.deserialize(
        ConsumerRecordHelper.serialize(record));

    assertRecordsAreEqual(record, newRecord);
  }

  @Test
  public void mapFns() throws IOException {
    ConsumerRecord<BytesWritable, BytesWritable> record = new ConsumerRecord<>("topic",
        1, 2, 3L, TimestampType.CREATE_TIME, 4L, 5,
        6, new BytesWritable("key".getBytes()), new BytesWritable("value".getBytes()));

    PCollection<BytesWritable> bytes = MemPipeline.collectionOf(new BytesWritable(ConsumerRecordHelper.serialize(record)));
    PCollection<ConsumerRecord<BytesWritable, BytesWritable>> records  = bytes.parallelDo(
        new ConsumerRecordHelper.BytesToConsumerRecord(), ConsumerRecordHelper.CONSUMER_RECORD_P_TYPE);
    PCollection<BytesWritable> newBytes  = records.parallelDo(
        new ConsumerRecordHelper.ConsumerRecordToBytes(), Writables.writables(BytesWritable.class));

    ConsumerRecord<BytesWritable, BytesWritable> newRecord = ConsumerRecordHelper.deserialize(
        newBytes.materialize().iterator().next().getBytes());

    assertRecordsAreEqual(record, newRecord);
  }

  private void assertRecordsAreEqual(ConsumerRecord<BytesWritable, BytesWritable> record1,
      ConsumerRecord<BytesWritable, BytesWritable> record2) {
    // ConsumerRecord doesn't implement equals so have to verify each field
    assertThat(record1.topic(), is(record2.topic()));
    assertThat(record1.partition(), is(record2.partition()));
    assertThat(record1.offset(), is(record2.offset()));
    assertThat(record1.timestamp(), is(record2.timestamp()));
    assertThat(record1.timestampType(), is(record2.timestampType()));
    assertThat(record1.checksum(), is(record2.checksum()));
    assertThat(record1.serializedKeySize(), is(record2.serializedKeySize()));
    assertThat(record1.serializedValueSize(), is(record2.serializedValueSize()));
    assertThat(record1.key(), is(record2.key()));
    assertThat(record1.value(), is(record2.value()));
  }
}
