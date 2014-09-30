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
package org.apache.crunch.types;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.protobuf.ExtensionRegistry;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.util.SerializableSupplier;
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

  /**
   * A PType for Java's {@link BigInteger} type.
   */
  public static PType<BigInteger> bigInt(PTypeFamily typeFamily) {
    return typeFamily.derivedImmutable(BigInteger.class, BYTE_TO_BIGINT, BIGINT_TO_BYTE, typeFamily.bytes());
  }

  /**
   * A PType for Java's {@link UUID} type.
   */
  public static PType<UUID> uuid(PTypeFamily ptf) {
    return ptf.derivedImmutable(UUID.class, BYTE_TO_UUID, UUID_TO_BYTE, ptf.bytes());
  }

  /**
   * Constructs a PType for reading a Java type from a JSON string using Jackson's {@link ObjectMapper}.
   */
  public static <T> PType<T> jsonString(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily
        .derived(clazz, new JacksonInputMapFn<T>(clazz), new JacksonOutputMapFn<T>(), typeFamily.strings());
  }

  /**
   * Constructs a PType for the given protocol buffer.
   */
  public static <T extends Message> PType<T> protos(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derivedImmutable(clazz, new ProtoInputMapFn<T>(clazz), new ProtoOutputMapFn<T>(), typeFamily.bytes());
  }

  /**
   * Constructs a PType for a protocol buffer, using the given {@code SerializableSupplier} to provide
   * an {@link ExtensionRegistry} to use in reading the given protobuf.
   */
  public static <T extends Message> PType<T> protos(
      Class<T> clazz,
      PTypeFamily typeFamily,
      SerializableSupplier<ExtensionRegistry> supplier) {
    return typeFamily.derivedImmutable(clazz,
        new ProtoInputMapFn<T>(clazz, supplier),
        new ProtoOutputMapFn<T>(),
        typeFamily.bytes());
  }

  /**
   * Constructs a PType for a Thrift record.
   */
  public static <T extends TBase> PType<T> thrifts(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derived(clazz, new ThriftInputMapFn<T>(clazz), new ThriftOutputMapFn<T>(), typeFamily.bytes());
  }

  /**
   * Constructs a PType for a Java {@code Enum} type.
   */
  public static <T extends Enum> PType<T> enums(Class<T> type, PTypeFamily typeFamily) {
    return typeFamily.derivedImmutable(type, new EnumInputMapper<T>(type), new EnumOutputMapper<T>(),
        typeFamily.strings());
  }

  public static final MapFn<ByteBuffer, BigInteger> BYTE_TO_BIGINT = new MapFn<ByteBuffer, BigInteger>() {
    @Override
    public BigInteger map(ByteBuffer input) {
      return input == null ? null : new BigInteger(input.array());
    }
  };

  public static final MapFn<BigInteger, ByteBuffer> BIGINT_TO_BYTE = new MapFn<BigInteger, ByteBuffer>() {
    @Override
    public ByteBuffer map(BigInteger input) {
      return input == null ? null : ByteBuffer.wrap(input.toByteArray());
    }
  };

  private static class JacksonInputMapFn<T> extends MapFn<String, T> {

    private final Class<T> clazz;
    private transient ObjectMapper mapper;

    JacksonInputMapFn(Class<T> clazz) {
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

  private static class JacksonOutputMapFn<T> extends MapFn<T, String> {

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

  private static class ProtoInputMapFn<T extends Message> extends MapFn<ByteBuffer, T> {

    private final Class<T> clazz;
    private final SerializableSupplier<ExtensionRegistry> extensionSupplier;
    private transient T instance;
    private transient ExtensionRegistry registry;

    ProtoInputMapFn(Class<T> clazz) {
      this(clazz, null);
    }

    ProtoInputMapFn(Class<T> clazz, SerializableSupplier<ExtensionRegistry> extensionSupplier) {
      this.clazz = clazz;
      this.extensionSupplier = extensionSupplier;
    }

    @Override
    public void initialize() {
      this.instance = Protos.getDefaultInstance(clazz);
      if (this.extensionSupplier != null) {
        this.registry = extensionSupplier.get();
      } else {
        this.registry = ExtensionRegistry.getEmptyRegistry();
      }
    }

    @Override
    public T map(ByteBuffer bb) {
      try {
        return (T) instance.newBuilderForType().mergeFrom(bb.array(), bb.position(), bb.limit(), registry).build();
      } catch (InvalidProtocolBufferException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  private static class ProtoOutputMapFn<T extends Message> extends MapFn<T, ByteBuffer> {

    ProtoOutputMapFn() {
    }

    @Override
    public ByteBuffer map(T proto) {
      return ByteBuffer.wrap(proto.toByteArray());
    }
  }

  private static class ThriftInputMapFn<T extends TBase> extends MapFn<ByteBuffer, T> {

    private final Class<T> clazz;
    private transient T instance;
    private transient TDeserializer deserializer;
    private transient byte[] bytes;

    ThriftInputMapFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.instance = ReflectionUtils.newInstance(clazz, null);
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

  private static class ThriftOutputMapFn<T extends TBase> extends MapFn<T, ByteBuffer> {

    private transient TSerializer serializer;

    ThriftOutputMapFn() {
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

  private static class EnumInputMapper<T extends Enum> extends MapFn<String, T> {
    private final Class<T> type;

    EnumInputMapper(Class<T> type) {
      this.type = type;
    }

    @Override
    public T map(String input) {
      return (T) Enum.valueOf(type, input);
    }
  }

  private static class EnumOutputMapper<T extends Enum> extends MapFn<T, String> {

    @Override
    public String map(T input) {
      return input.name();
    }
  }

  private static final MapFn<ByteBuffer, UUID> BYTE_TO_UUID = new MapFn<ByteBuffer, UUID>() {
    @Override
    public UUID map(ByteBuffer input) {
      return new UUID(input.getLong(), input.getLong());
    }
  };
  
  private static final MapFn<UUID, ByteBuffer> UUID_TO_BYTE = new MapFn<UUID, ByteBuffer>() {
    @Override
    public ByteBuffer map(UUID input) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.asLongBuffer().put(input.getMostSignificantBits()).put(input.getLeastSignificantBits());
      return bb;
    }
  };
}
