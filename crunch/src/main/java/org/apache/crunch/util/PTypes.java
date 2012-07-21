/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.crunch.MapFn;
import org.apache.crunch.impl.mr.run.CrunchRuntimeException;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Utility functions for creating common types of derived PTypes, e.g., for JSON
 * data, protocol buffers, and Thrift records.
 * 
 */
public class PTypes {

  public static PType<BigInteger> bigInt(PTypeFamily typeFamily) {
    return typeFamily.derived(BigInteger.class, BYTE_TO_BIGINT, BIGINT_TO_BYTE, typeFamily.bytes());
  }

  public static <T> PType<T> jsonString(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily
        .derived(clazz, new JacksonInputMapFn<T>(clazz), new JacksonOutputMapFn<T>(), typeFamily.strings());
  }

  public static <T extends Message> PType<T> protos(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derived(clazz, new ProtoInputMapFn<T>(clazz), new ProtoOutputMapFn<T>(), typeFamily.bytes());
  }

  public static <T extends TBase> PType<T> thrifts(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derived(clazz, new ThriftInputMapFn<T>(clazz), new ThriftOutputMapFn<T>(), typeFamily.bytes());
  }

  public static MapFn<ByteBuffer, BigInteger> BYTE_TO_BIGINT = new MapFn<ByteBuffer, BigInteger>() {
    public BigInteger map(ByteBuffer input) {
      return input == null ? null : new BigInteger(input.array());
    }
  };

  public static MapFn<BigInteger, ByteBuffer> BIGINT_TO_BYTE = new MapFn<BigInteger, ByteBuffer>() {
    public ByteBuffer map(BigInteger input) {
      return input == null ? null : ByteBuffer.wrap(input.toByteArray());
    }
  };

  public static class JacksonInputMapFn<T> extends MapFn<String, T> {

    private final Class<T> clazz;
    private transient ObjectMapper mapper;

    public JacksonInputMapFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public T map(String input) {
      try {
        return mapper.readValue(input, clazz);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  public static class JacksonOutputMapFn<T> extends MapFn<T, String> {

    private transient ObjectMapper mapper;

    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public String map(T input) {
      try {
        return mapper.writeValueAsString(input);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  public static class ProtoInputMapFn<T extends Message> extends MapFn<ByteBuffer, T> {

    private final Class<T> clazz;
    private transient T instance;

    public ProtoInputMapFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.instance = ReflectionUtils.newInstance(clazz, getConfiguration());
    }

    @Override
    public T map(ByteBuffer bb) {
      try {
        return (T) instance.newBuilderForType().mergeFrom(bb.array(), bb.position(), bb.limit()).build();
      } catch (InvalidProtocolBufferException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  public static class ProtoOutputMapFn<T extends Message> extends MapFn<T, ByteBuffer> {

    public ProtoOutputMapFn() {
    }

    @Override
    public ByteBuffer map(T proto) {
      return ByteBuffer.wrap(proto.toByteArray());
    }
  }

  public static class ThriftInputMapFn<T extends TBase> extends MapFn<ByteBuffer, T> {

    private final Class<T> clazz;
    private transient T instance;
    private transient TDeserializer deserializer;
    private transient byte[] bytes;

    public ThriftInputMapFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.instance = ReflectionUtils.newInstance(clazz, getConfiguration());
      this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      this.bytes = new byte[0];
    }

    @Override
    public T map(ByteBuffer bb) {
      T next = (T) instance.deepCopy();
      int len = bb.limit() - bb.position();
      if (len != bytes.length) {
        bytes = new byte[len];
      }
      System.arraycopy(bb.array(), bb.position(), bytes, 0, len);
      try {
        deserializer.deserialize(next, bytes);
      } catch (TException e) {
        throw new CrunchRuntimeException(e);
      }
      return next;
    }
  }

  public static class ThriftOutputMapFn<T extends TBase> extends MapFn<T, ByteBuffer> {

    private transient TSerializer serializer;

    public ThriftOutputMapFn() {
    }

    @Override
    public void initialize() {
      this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    }

    @Override
    public ByteBuffer map(T t) {
      try {
        return ByteBuffer.wrap(serializer.serialize(t));
      } catch (TException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }
}
