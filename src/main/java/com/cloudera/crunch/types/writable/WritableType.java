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
package com.cloudera.crunch.types.writable;

import java.util.List;

import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.seq.SeqFileSourceTarget;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.google.common.collect.ImmutableList;

public class WritableType<T, W> implements PType<T> {

  private final Class<T> typeClass;
  private final Class<W> writableClass;
  private final Converter converter;
  private final MapFn<W, T> inputFn;
  private final MapFn<T, W> outputFn;
  private final List<PType> subTypes;
  
  WritableType(Class<T> typeClass, Class<W> writableClass,
      MapFn<W, T> inputDoFn, MapFn<T, W> outputDoFn, PType...subTypes) {
    this.typeClass = typeClass;
    this.writableClass = writableClass;
    this.inputFn = inputDoFn;
    this.outputFn = outputDoFn;
    this.converter = new WritableValueConverter(writableClass);
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
	return (typeClass.equals(wt.typeClass) && writableClass.equals(wt.writableClass) &&	
		subTypes.equals(wt.subTypes));
  }
  
  @Override
  public int hashCode() {
	HashCodeBuilder hcb = new HashCodeBuilder();
	hcb.append(typeClass).append(writableClass).append(subTypes);
	return hcb.toHashCode();
  }
}