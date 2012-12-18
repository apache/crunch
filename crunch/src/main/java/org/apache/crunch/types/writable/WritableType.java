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
package org.apache.crunch.types.writable;

import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.seq.SeqFileSourceTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.DeepCopier;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.ImmutableList;

public class WritableType<T, W extends Writable> implements PType<T> {

  private final Class<T> typeClass;
  private final Class<W> writableClass;
  private final Converter converter;
  private final MapFn<W, T> inputFn;
  private final MapFn<T, W> outputFn;
  private final DeepCopier<W> deepCopier;
  private final List<PType> subTypes;
  private boolean initialized = false;

  public WritableType(Class<T> typeClass, Class<W> writableClass, MapFn<W, T> inputDoFn,
      MapFn<T, W> outputDoFn, PType... subTypes) {
    this.typeClass = typeClass;
    this.writableClass = writableClass;
    this.inputFn = inputDoFn;
    this.outputFn = outputDoFn;
    this.converter = new WritableValueConverter(writableClass);
    this.deepCopier = new WritableDeepCopier<W>(writableClass);
    this.subTypes = ImmutableList.<PType> builder().add(subTypes).build();
  }

  @Override
  public PTypeFamily getFamily() {
    return WritableTypeFamily.getInstance();
  }

  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }

  @Override
  public Converter getConverter() {
    return converter;
  }

  @Override
  public MapFn getInputMapFn() {
    return inputFn;
  }

  @Override
  public MapFn getOutputMapFn() {
    return outputFn;
  }

  @Override
  public List<PType> getSubTypes() {
    return subTypes;
  }

  public Class<W> getSerializationClass() {
    return writableClass;
  }

  @Override
  public SourceTarget<T> getDefaultFileSource(Path path) {
    return new SeqFileSourceTarget<T>(path, this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof WritableType)) {
      return false;
    }
    WritableType wt = (WritableType) obj;
    return (typeClass.equals(wt.typeClass) && writableClass.equals(wt.writableClass) && subTypes
        .equals(wt.subTypes));
  }

  @Override
  public void initialize(Configuration conf) {
    this.inputFn.initialize();
    this.outputFn.initialize();
    for (PType subType : subTypes) {
      subType.initialize(conf);
    }
    this.initialized = true;
  }

  @Override
  public T getDetachedValue(T value) {
    if (!initialized) {
      throw new IllegalStateException("Cannot call getDetachedValue on an uninitialized PType");
    }
    W writableValue = outputFn.map(value);
    W deepCopy = this.deepCopier.deepCopy(writableValue);
    return inputFn.map(deepCopy);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(typeClass).append(writableClass).append(subTypes);
    return hcb.toHashCode();
  }
}