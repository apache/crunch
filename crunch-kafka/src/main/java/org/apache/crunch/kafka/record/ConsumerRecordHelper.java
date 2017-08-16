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

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer/De-Serializer for Kafka's {@link ConsumerRecord}
 */
public class ConsumerRecordHelper {

  /**
   * PType for {@link ConsumerRecord}
   */
  @SuppressWarnings("unchecked")
  public static final PType<ConsumerRecord<BytesWritable, BytesWritable>> CONSUMER_RECORD_P_TYPE = Writables
      .derived((Class<ConsumerRecord<BytesWritable, BytesWritable>>) (Object) ConsumerRecord.class,
          new ConsumerRecordHelper.BytesToConsumerRecord(), new ConsumerRecordHelper.ConsumerRecordToBytes(),
          Writables.writables(BytesWritable.class));

  /**
   * Serializes the record into {@code byte[]}s
   *
   * @param record the record to serialize
   * @return the record in {@code byte[]}s
   * @throws IllegalArgumentException if record is {@code null}
   * @throws IOException              if there is an issue during serialization
   */
  public static byte[] serialize(ConsumerRecord<BytesWritable, BytesWritable> record) throws IOException {
    if (record == null)
      throw new IllegalArgumentException("record cannot be null");

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

    try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
      dataOut.writeUTF(record.topic());
      dataOut.writeInt(record.partition());
      dataOut.writeLong(record.offset());
      dataOut.writeLong(record.timestamp());
      dataOut.writeUTF(record.timestampType().name);
      dataOut.writeLong(record.checksum());
      dataOut.writeInt(record.serializedKeySize());
      dataOut.writeInt(record.serializedValueSize());

      if (record.key() == null) {
        dataOut.writeInt(-1);
      } else {
        byte[] keyBytes = record.key().getBytes();
        dataOut.writeInt(keyBytes.length);
        dataOut.write(keyBytes);
      }

      if (record.value() == null) {
        dataOut.writeInt(-1);
      } else {
        byte[] valueBytes = record.value().getBytes();
        dataOut.writeInt(valueBytes.length);
        dataOut.write(valueBytes);
      }

      return byteOut.toByteArray();
    }
  }

  /**
   * De-serializes the bytes into a {@link ConsumerRecord}
   *
   * @param bytes the bytes of a {@link ConsumerRecord}
   * @return a {@link ConsumerRecord} from the bytes
   * @throws IllegalArgumentException if bytes is {@code null}
   * @throws IOException              if there is an issue de-serializing the bytes
   */
  public static ConsumerRecord<BytesWritable, BytesWritable> deserialize(byte[] bytes) throws IOException {
    if (bytes == null)
      throw new IllegalArgumentException("bytes cannot be null");

    try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes))) {
      String topic = dataIn.readUTF();
      int partition = dataIn.readInt();
      long offset = dataIn.readLong();
      long timestamp = dataIn.readLong();
      String timestampTypeName = dataIn.readUTF();
      long checksum = dataIn.readLong();
      int serializedKeySize = dataIn.readInt();
      int serializedValueSize = dataIn.readInt();

      BytesWritable key = null;
      int keySize = dataIn.readInt();
      if (keySize != -1) {
        byte[] keyBytes = new byte[keySize];
        dataIn.readFully(keyBytes);
        key = new BytesWritable(keyBytes);
      }

      BytesWritable value = null;
      int valueSize = dataIn.readInt();
      if (valueSize != -1) {
        byte[] valueBytes = new byte[valueSize];
        dataIn.readFully(valueBytes);
        value = new BytesWritable(valueBytes);
      }

      return new ConsumerRecord<>(topic, partition, offset, timestamp, TimestampType.forName(timestampTypeName), checksum,
          serializedKeySize, serializedValueSize, key, value);
    }
  }

  /**
   * {@link MapFn} to convert {@link ConsumerRecord} to {@link BytesWritable}
   */
  public static class ConsumerRecordToBytes extends MapFn<ConsumerRecord<BytesWritable, BytesWritable>, BytesWritable> {
    private static final long serialVersionUID = -6821080008375335537L;

    @Override
    public BytesWritable map(ConsumerRecord<BytesWritable, BytesWritable> record) {
      try {
        return new BytesWritable(ConsumerRecordHelper.serialize(record));
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error serializing consumer record " + record, e);
      }
    }
  }

  /**
   * {@link MapFn} to convert {@link BytesWritable} to {@link ConsumerRecord}
   */
  public static class BytesToConsumerRecord extends MapFn<BytesWritable, ConsumerRecord<BytesWritable, BytesWritable>> {
    private static final long serialVersionUID = -6545017910063252322L;

    @Override
    public ConsumerRecord<BytesWritable, BytesWritable> map(BytesWritable bytesWritable) {
      try {
        return ConsumerRecordHelper.deserialize(bytesWritable.getBytes());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error deserializing consumer record", e);
      }
    }
  }
}
