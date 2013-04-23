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
package org.apache.crunch.types.avro;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.types.DeepCopier;
import org.apache.hadoop.conf.Configuration;

/**
 * Performs deep copies of Avro-serializable objects.
 * <p>
 * <b>Warning:</b> Methods in this class are not thread-safe. This shouldn't be a problem when
 * running in a map-reduce context where each mapper/reducer is running in its own JVM, but it may
 * well be a problem in any other kind of multi-threaded context.
 */
abstract class AvroDeepCopier<T> implements DeepCopier<T>, Serializable {

  private String jsonSchema;
  private transient Configuration conf;
  private transient Schema schema;
  private BinaryEncoder binaryEncoder;
  private BinaryDecoder binaryDecoder;

  private transient DatumWriter<T> datumWriter;
  private transient DatumReader<T> datumReader;

  public AvroDeepCopier(Schema schema) {
    this.jsonSchema = schema.toString();
  }

  protected Schema getSchema() {
    if (schema == null) {
      schema = new Schema.Parser().parse(jsonSchema);
    }
    return schema;
  }

  @Override
  public void initialize(Configuration conf) {
    this.conf = conf;
  }

  protected abstract T createCopyTarget();

  protected abstract DatumWriter<T> createDatumWriter(Configuration conf);

  protected abstract DatumReader<T> createDatumReader(Configuration conf);

  /**
   * Deep copier for Avro specific data objects.
   */
  public static class AvroSpecificDeepCopier<T> extends AvroDeepCopier<T> {

    private Class<T> valueClass;

    public AvroSpecificDeepCopier(Class<T> valueClass, Schema schema) {
      super(schema);
      this.valueClass = valueClass;
    }

    @Override
    protected T createCopyTarget() {
      return createNewInstance(valueClass);
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Configuration conf) {
      return new SpecificDatumWriter<T>(getSchema());
    }

    @Override
    protected DatumReader<T> createDatumReader(Configuration conf) {
      return new SpecificDatumReader<T>(getSchema());
    }

  }

  /**
   * Deep copier for Avro generic data objects.
   */
  public static class AvroGenericDeepCopier extends AvroDeepCopier<Record> {

    private transient Schema schema;

    public AvroGenericDeepCopier(Schema schema) {
      super(schema);
    }

    @Override
    protected Record createCopyTarget() {
      return new GenericData.Record(getSchema());
    }

    @Override
    protected DatumReader<Record> createDatumReader(Configuration conf) {
      return new GenericDatumReader<Record>(getSchema());
    }

    @Override
    protected DatumWriter<Record> createDatumWriter(Configuration conf) {
      return new GenericDatumWriter<Record>(getSchema());
    }
  }

  /**
   * Deep copier for Avro reflect data objects.
   */
  public static class AvroReflectDeepCopier<T> extends AvroDeepCopier<T> {

    private Class<T> valueClass;

    public AvroReflectDeepCopier(Class<T> valueClass, Schema schema) {
      super(schema);
      this.valueClass = valueClass;
    }

    @Override
    protected T createCopyTarget() {
      return createNewInstance(valueClass);
    }

    @Override
    protected DatumReader<T> createDatumReader(Configuration conf) {
      return Avros.getReflectDataFactory(conf).getReader(getSchema());
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Configuration conf) {
      return Avros.getReflectDataFactory(conf).getWriter(getSchema());
    }
  }

  /**
   * Create a deep copy of an Avro value.
   * 
   * @param source The value to be copied
   * @return The deep copy of the value
   */
  @Override
  public T deepCopy(T source) {
    
    if (source == null) {
      return null;
    }
    
    if (datumReader == null) {
      datumReader = createDatumReader(conf);
    }
    if (datumWriter == null) {
      datumWriter = createDatumWriter(conf);
    }
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    binaryEncoder = EncoderFactory.get().binaryEncoder(byteOutStream, binaryEncoder);
    T target = createCopyTarget();
    try {
      datumWriter.write(source, binaryEncoder);
      binaryEncoder.flush();
      binaryDecoder = DecoderFactory.get()
          .binaryDecoder(byteOutStream.toByteArray(), binaryDecoder);
      datumReader.read(target, binaryDecoder);
    } catch (Exception e) {
      throw new CrunchRuntimeException("Error while deep copying avro value " + source, e);
    }

    return target;
  }

  protected T createNewInstance(Class<T> targetClass) {
    try {
      return targetClass.newInstance();
    } catch (InstantiationException e) {
      throw new CrunchRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new CrunchRuntimeException(e);
    }
  }

}
