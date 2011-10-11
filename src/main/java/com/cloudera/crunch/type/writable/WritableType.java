/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.type.writable;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.seq.SeqFileSourceTarget;
import com.cloudera.crunch.type.Converter;
import com.cloudera.crunch.type.DataBridge;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class WritableType<T, W extends Writable> implements PType<T> {

  private static Converter WRITABLE_CONVERTER = new WritableValueConverter();
  
  private final Class<T> typeClass;
  private final Class<W> writableClass;
  private final DataBridge handler;
  private final List<PType> subTypes;
  
  WritableType(Class<T> typeClass, Class<W> writableClass,
      MapFn<W, T> inputDoFn, MapFn<T, W> outputDoFn, PType...subTypes) {
    this.typeClass = typeClass;
    this.writableClass = writableClass;
    this.handler = new DataBridge(NullWritable.class, writableClass, WRITABLE_CONVERTER,
        inputDoFn, outputDoFn);
    this.subTypes = ImmutableList.<PType>builder().add(subTypes).build();
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
  public List<PType> getSubTypes() {
    return subTypes;
  }
  
  public Class<W> getSerializationClass() {
    return writableClass;
  }

  @Override
  public DataBridge getDataBridge() {
    return handler;
  }

  @Override
  public SourceTarget<T> getDefaultFileSource(Path path) {
    return new SeqFileSourceTarget<T>(path, this);
  }  
}