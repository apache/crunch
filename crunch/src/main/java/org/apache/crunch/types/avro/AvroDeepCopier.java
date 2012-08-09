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
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.impl.mr.run.CrunchRuntimeException;
import org.apache.crunch.types.DeepCopier;

/**
 * Performs deep copies of Avro-serializable objects.
 * <p>
 * <b>Warning:</b> Methods in this class are not thread-safe. This shouldn't be
 * a problem when running in a map-reduce context where each mapper/reducer is
 * running in its own JVM, but it may well be a problem in any other kind of
 * multi-threaded context.
 */
public abstract class AvroDeepCopier<T> implements DeepCopier<T>, Serializable {

  private BinaryEncoder binaryEncoder;
  private BinaryDecoder binaryDecoder;
  protected DatumWriter<T> datumWriter;
  protected DatumReader<T> datumReader;

  protected AvroDeepCopier(DatumWriter<T> datumWriter, DatumReader<T> datumReader) {
    this.datumWriter = datumWriter;
    this.datumReader = datumReader;
  }

  protected abstract T createCopyTarget();

  /**
   * Deep copier for Avro specific data objects.
   */
  public static class AvroSpecificDeepCopier<T> extends AvroDeepCopier<T> {

    private Class<T> valueClass;

    public AvroSpecificDeepCopier(Class<T> valueClass, Schema schema) {
      super(new SpecificDatumWriter<T>(schema), new SpecificDatumReader<T>(schema));
      this.valueClass = valueClass;
    }

    @Override
    protected T createCopyTarget() {
      return createNewInstance(valueClass);
    }

  }

  /**
   * Deep copier for Avro generic data objects.
   */
  public static class AvroGenericDeepCopier extends AvroDeepCopier<Record> {

    private Schema schema;

    public AvroGenericDeepCopier(Schema schema) {
      super(new GenericDatumWriter<Record>(schema), new GenericDatumReader<Record>(schema));
      this.schema = schema;
    }

    @Override
    protected Record createCopyTarget() {
      return new GenericData.Record(schema);
    }
  }

  /**
   * Deep copier for Avro reflect data objects.
   */
  public static class AvroReflectDeepCopier<T> extends AvroDeepCopier<T> {
    private Class<T> valueClass;

    public AvroReflectDeepCopier(Class<T> valueClass, Schema schema) {
      super(new ReflectDatumWriter<T>(schema), new ReflectDatumReader<T>(schema));
      this.valueClass = valueClass;
    }

    @Override
    protected T createCopyTarget() {
      return createNewInstance(valueClass);
    }
  }

  public static class AvroTupleDeepCopier {

  }

  /**
   * Create a deep copy of an Avro value.
   * 
   * @param source
   *          The value to be copied
   * @return The deep copy of the value
   */
  @Override
  public T deepCopy(T source) {
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
