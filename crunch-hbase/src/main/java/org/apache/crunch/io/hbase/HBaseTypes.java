/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch.io.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class HBaseTypes {

  public static final PType<Put> puts() {
    return Writables.derived(Put.class,
        new MapInFn<Put>(Put.class, MutationSerialization.class),
        new MapOutFn<Put>(Put.class, MutationSerialization.class),
        Writables.bytes());
  }

  public static final PType<Delete> deletes() {
    return Writables.derived(Delete.class,
        new MapInFn<Delete>(Delete.class, MutationSerialization.class),
        new MapOutFn<Delete>(Delete.class, MutationSerialization.class),
        Writables.bytes());
  }

  public static final PType<Result> results() {
    return Writables.derived(Result.class,
        new MapInFn<Result>(Result.class, ResultSerialization.class),
        new MapOutFn<Result>(Result.class, ResultSerialization.class),
        Writables.bytes());
  }

  public static final PType<KeyValue> keyValues() {
    return Writables.derived(KeyValue.class,
        new MapFn<BytesWritable, KeyValue>() {
          @Override
          public KeyValue map(BytesWritable input) {
            return bytesToKeyValue(input);
          }
        },
        new MapFn<KeyValue, BytesWritable>() {
          @Override
          public BytesWritable map(KeyValue input) {
            return keyValueToBytes(input);
          }
        },
        Writables.writables(BytesWritable.class));
  }

  public static final PType<Cell> cells() {
    return Writables.derived(Cell.class,
        new MapFn<BytesWritable, Cell>() {
          @Override
          public Cell map(BytesWritable input) {
            return bytesToKeyValue(input);
          }
        },
        new MapFn<Cell, BytesWritable>() {
          @Override
          public BytesWritable map(Cell input) {
            return keyValueToBytes(input);
          }
        },
        Writables.writables(BytesWritable.class));
  }

  public static BytesWritable keyValueToBytes(Cell input) {
    return keyValueToBytes(KeyValueUtil.copyToNewKeyValue(input));
  }

  public static BytesWritable keyValueToBytes(KeyValue kv) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      KeyValue.write(kv, dos);
      return new BytesWritable(baos.toByteArray());
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }

  public static KeyValue bytesToKeyValue(BytesWritable input) {
    return bytesToKeyValue(input.getBytes(), 0, input.getLength());
  }

  public static KeyValue bytesToKeyValue(byte[] array, int offset, int limit) {
    ByteArrayInputStream bais = new ByteArrayInputStream(array, offset, limit);
    DataInputStream dis = new DataInputStream(bais);
    try {
      return KeyValue.create(dis);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  private static class MapInFn<T> extends MapFn<ByteBuffer, T> {
    private Class<T> clazz;
    private Class<? extends Serialization> serClazz;
    private transient Deserializer<T> deserializer;

    public MapInFn(Class<T> clazz, Class<? extends Serialization> serClazz) {
      this.clazz = clazz;
      this.serClazz = serClazz;
    }

    @Override
    public void initialize() {
      this.deserializer = ReflectionUtils.newInstance(serClazz, null).getDeserializer(clazz);
      if (deserializer == null) {
        throw new CrunchRuntimeException("No Hadoop deserializer for class: " + clazz);
      }
    }

    @Override
    public T map(ByteBuffer bb) {
      if (deserializer == null) {
        initialize();
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(bb.array(), bb.position(), bb.limit());
      try {
        deserializer.open(bais);
        T ret = deserializer.deserialize(null);
        deserializer.close();
        return ret;
      } catch (Exception e) {
        throw new CrunchRuntimeException("Deserialization errror", e);
      }
    }
  }

  private static class MapOutFn<T> extends MapFn<T, ByteBuffer> {
    private Class<T> clazz;
    private Class<? extends Serialization> serClazz;
    private transient Serializer<T> serializer;

    public MapOutFn(Class<T> clazz, Class<? extends Serialization> serClazz) {
      this.clazz = clazz;
      this.serClazz = serClazz;
    }

    @Override
    public void initialize() {
      this.serializer = ReflectionUtils.newInstance(serClazz, null).getSerializer(clazz);
      if (serializer == null) {
        throw new CrunchRuntimeException("No Hadoop serializer for class: " + clazz);
      }
    }

    @Override
    public ByteBuffer map(T out) {
      if (serializer == null) {
        initialize();
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        serializer.open(baos);
        serializer.serialize(out);
        serializer.close();
        return ByteBuffer.wrap(baos.toByteArray());
      } catch (Exception e) {
        throw new CrunchRuntimeException("Serialization errror", e);
      }
    }
  }

  private HBaseTypes() {}
}
