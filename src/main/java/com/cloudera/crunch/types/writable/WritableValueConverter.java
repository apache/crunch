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

import com.cloudera.crunch.types.Converter;
import org.apache.hadoop.io.NullWritable;

import com.cloudera.crunch.types.Converter;

class WritableValueConverter<W> implements Converter<Object, W, W, Iterable<W>> {

  private final Class<W> serializationClass;
  
  public WritableValueConverter(Class<W> serializationClass) {
    this.serializationClass = serializationClass;
  }
  
  @Override
  public W convertInput(Object key, W value) {
    return value;
  }

  @Override
  public Object outputKey(W value) {
    return NullWritable.get();
  }

  @Override
  public W outputValue(W value) {
    return value;
  }

  @Override
  public Class<Object> getKeyClass() {
    return (Class<Object>) (Class<?>) NullWritable.class;
  }

  @Override
  public Class<W> getValueClass() {
    return serializationClass;
  }

  @Override
  public Iterable<W> convertIterableInput(Object key, Iterable<W> value) {
    return value;
  }
}