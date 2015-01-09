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
package org.apache.crunch.impl.spark.serde;

import com.google.common.base.Function;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.ByteArrayHelper;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerDe<T> implements SerDe<T> {

  private AvroType<T> avroType;
  private Map<String, String> modeProperties;
  private ByteArrayHelper helper;
  private transient AvroMode mode;
  private transient DatumWriter<T> writer;
  private transient DatumReader<T> reader;

  public AvroSerDe(AvroType<T> avroType, Map<String, String> modeProperties) {
    this.avroType = avroType;
    this.modeProperties = modeProperties;
    if (avroType.hasReflect() && avroType.hasSpecific()) {
      Avros.checkCombiningSpecificAndReflectionSchemas();
    }
    this.helper = ByteArrayHelper.forAvroSchema(avroType.getSchema());
  }

  private AvroMode getMode() {
    if (mode == null) {
      mode = AvroMode.fromType(avroType);
      if (modeProperties != null && !modeProperties.isEmpty()) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : modeProperties.entrySet()) {
          conf.set(e.getKey(), e.getValue());
        }
        mode = mode.withFactoryFromConfiguration(conf);
      }
    }
    return mode;
  }

  private DatumWriter<T> getWriter() {
    if (writer == null) {
      writer = getMode().getWriter(avroType.getSchema());
    }
    return writer;
  }

  private DatumReader<T> getReader() {
    if (reader == null) {
      reader = getMode().getReader(avroType.getSchema());
    }
    return reader;
  }

  @Override
  public ByteArray toBytes(T obj) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    getWriter().write(obj, encoder);
    encoder.flush();
    out.close();
    return new ByteArray(out.toByteArray(), helper);
  }

  @Override
  public T fromBytes(byte[] bytes) {
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    try {
      return getReader().read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Function<byte[], T> fromBytesFunction() {
    return new Function<byte[], T>() {
      @Override
      public T apply(@Nullable byte[] input) {
        return fromBytes(input);
      }
    };
  }
}
