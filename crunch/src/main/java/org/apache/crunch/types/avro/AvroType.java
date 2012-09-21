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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.io.avro.AvroFileSourceTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.DeepCopier;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * The implementation of the PType interface for Avro-based serialization.
 * 
 */
public class AvroType<T> implements PType<T> {

  private static final Converter AVRO_CONVERTER = new AvroKeyConverter();

  private final Class<T> typeClass;
  private final String schemaString;
  private transient Schema schema;
  private final MapFn baseInputMapFn;
  private final MapFn baseOutputMapFn;
  private final List<PType> subTypes;
  private DeepCopier<T> deepCopier;

  public AvroType(Class<T> typeClass, Schema schema, DeepCopier<T> deepCopier, PType... ptypes) {
    this(typeClass, schema, IdentityFn.getInstance(), IdentityFn.getInstance(), deepCopier, ptypes);
  }

  public AvroType(Class<T> typeClass, Schema schema, MapFn inputMapFn, MapFn outputMapFn, DeepCopier<T> deepCopier,
      PType... ptypes) {
    this.typeClass = typeClass;
    this.schema = Preconditions.checkNotNull(schema);
    this.schemaString = schema.toString();
    this.baseInputMapFn = inputMapFn;
    this.baseOutputMapFn = outputMapFn;
    this.deepCopier = deepCopier;
    this.subTypes = ImmutableList.<PType> builder().add(ptypes).build();
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
    if (Avros.isPrimitive(this)) {
      return false;
    }
    
    if (!this.subTypes.isEmpty()) {
      for (PType<?> subType : this.subTypes) {
        AvroType<?> atype = (AvroType<?>) subType;
        if (atype.hasSpecific()) {
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
    if (Avros.isPrimitive(this)) {
      return false;
    }

    if (!this.subTypes.isEmpty()) {
      for (PType<?> subType : this.subTypes) {
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
  public SourceTarget<T> getDefaultFileSource(Path path) {
    return new AvroFileSourceTarget<T>(path, this);
  }

  @Override
  public void initialize() {
    // No initialization needed for Avro PTypes
  }

  public T getDetachedValue(T value) {
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
