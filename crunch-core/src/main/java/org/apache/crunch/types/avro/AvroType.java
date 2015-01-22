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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.avro.AvroFileSourceTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.DeepCopier;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * The implementation of the PType interface for Avro-based serialization.
 * 
 */
public class AvroType<T> implements PType<T> {

  public enum AvroRecordType {
    REFLECT,
    SPECIFIC,
    GENERIC
  }

  private static final Logger LOG = LoggerFactory.getLogger(AvroType.class);
  private static final Converter AVRO_CONVERTER = new AvroKeyConverter();

  private final Class<T> typeClass;
  private final String schemaString;
  private transient Schema schema;
  private final MapFn baseInputMapFn;
  private final MapFn baseOutputMapFn;
  private final List<PType> subTypes;
  private AvroRecordType recordType;
  private DeepCopier<T> deepCopier;
  private boolean initialized = false;

  public AvroType(Class<T> typeClass, Schema schema, DeepCopier<T> deepCopier, PType... ptypes) {
    this(typeClass, schema, IdentityFn.getInstance(), IdentityFn.getInstance(), deepCopier, null, ptypes);
  }

  public AvroType(Class<T> typeClass, Schema schema, MapFn inputMapFn, MapFn outputMapFn,
      DeepCopier<T> deepCopier, AvroRecordType recordType, PType... ptypes) {
    this.typeClass = typeClass;
    this.schema = Preconditions.checkNotNull(schema);
    this.schemaString = schema.toString();
    this.baseInputMapFn = inputMapFn;
    this.baseOutputMapFn = outputMapFn;
    this.deepCopier = deepCopier;
    this.subTypes = ImmutableList.<PType> builder().add(ptypes).build();
    this.recordType = recordType;
  }

  private AvroRecordType determineRecordType() {
    if (checkReflect()) {
      return AvroRecordType.REFLECT;
    } else if (checkSpecific()) {
      return AvroRecordType.SPECIFIC;
    }
    return AvroRecordType.GENERIC;
  }

  public AvroRecordType getRecordType() {
    if (recordType == null) {
      recordType = determineRecordType();
    }
    return recordType;
  }

  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }

  @Override
  public PTypeFamily getFamily() {
    return AvroTypeFamily.getInstance();
  }

  @Override
  public List<PType> getSubTypes() {
    return Lists.<PType> newArrayList(subTypes);
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = new Schema.Parser().parse(schemaString);
    }
    return schema;
  }

  /**
   * Determine if the wrapped type is a specific data avro type or wraps one.
   * 
   * @return true if the wrapped type is a specific data type or wraps one
   */
  public boolean hasSpecific() {
    return getRecordType() == AvroRecordType.SPECIFIC;
  }

  private boolean checkSpecific() {
    if (Avros.isPrimitive(typeClass)) {
      return false;
    }

    if (!subTypes.isEmpty()) {
      for (PType<?> subType : subTypes) {
        if (((AvroType<?>) subType).hasSpecific()) {
          return true;
        }
      }
      return false;
    }

    return SpecificRecord.class.isAssignableFrom(typeClass);
  }

  /**
   * Determine if the wrapped type is a generic data avro type.
   * 
   * @return true if the wrapped type is a generic type
   */
  public boolean isGeneric() {
    return GenericData.Record.class.equals(typeClass);
  }

  /**
   * Determine if the wrapped type is a reflection-based avro type or wraps one.
   * 
   * @return true if the wrapped type is a reflection-based type or wraps one.
   */
  public boolean hasReflect() {
    return getRecordType() == AvroRecordType.REFLECT;
  }

  private boolean checkReflect() {
    if (Avros.isPrimitive(typeClass)) {
      return false;
    }

    if (!subTypes.isEmpty()) {
      for (PType<?> subType : subTypes) {
        if (((AvroType<?>) subType).hasReflect()) {
          return true;
        }
      }
      return false;
    }

    return !(typeClass.equals(GenericData.Record.class) || SpecificRecord.class
        .isAssignableFrom(typeClass));
  }

  public MapFn<Object, T> getInputMapFn() {
    return baseInputMapFn;
  }

  public MapFn<T, Object> getOutputMapFn() {
    return baseOutputMapFn;
  }

  @Override
  public Converter getConverter() {
    return AVRO_CONVERTER;
  }

  @Override
  public ReadableSourceTarget<T> getDefaultFileSource(Path path) {
    return new AvroFileSourceTarget<T>(path, this);
  }

  @Override
  public ReadableSource<T> createSourceTarget(Configuration conf, Path path, Iterable<T> contents, int parallelism)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    baseOutputMapFn.setConfiguration(conf);
    baseOutputMapFn.initialize();
    fs.mkdirs(path);
    List<FSDataOutputStream> streams = Lists.newArrayListWithExpectedSize(parallelism);
    List<DataFileWriter> writers = Lists.newArrayListWithExpectedSize(parallelism);
    for (int i = 0; i < parallelism; i++) {
      Path out = new Path(path, "out" + i);
      FSDataOutputStream stream = fs.create(out);
      DatumWriter datumWriter = Avros.newWriter(this);
      DataFileWriter writer = new DataFileWriter(datumWriter);
      writer.create(getSchema(), stream);

      streams.add(stream);
      writers.add(writer);
    }
    int target = 0;
    for (T value : contents) {
      writers.get(target).append(baseOutputMapFn.map(value));
      target = (target + 1) % parallelism;
    }
    for (DataFileWriter writer : writers) {
      writer.close();
    }
    for (FSDataOutputStream stream : streams) {
      stream.close();
    }
    ReadableSource<T> ret = new AvroFileSource<T>(path, this);
    ret.inputConf(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    return ret;
  }

  @Override
  public void initialize(Configuration conf) {
    baseInputMapFn.setConfiguration(conf);
    baseInputMapFn.initialize();
    baseOutputMapFn.setConfiguration(conf);
    baseOutputMapFn.initialize();
    deepCopier.initialize(conf);
    for (PType ptype : subTypes) {
      ptype.initialize(conf);
    }
    initialized = true;
  }

  @Override
  public T getDetachedValue(T value) {
    if (!initialized) {
      throw new IllegalStateException("Cannot call getDetachedValue on an uninitialized PType");
    }
    return deepCopier.deepCopy(value);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof AvroType)) {
      return false;
    }
    AvroType at = (AvroType) other;
    return (typeClass.equals(at.typeClass) && subTypes.equals(at.subTypes));

  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(typeClass).append(subTypes);
    return hcb.toHashCode();
  }

}
