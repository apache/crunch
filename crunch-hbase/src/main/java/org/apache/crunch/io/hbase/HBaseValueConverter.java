/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch.io.hbase;

import org.apache.crunch.types.Converter;
import org.apache.hadoop.io.NullWritable;

public class HBaseValueConverter<V> implements Converter<Object, V, V, Iterable<V>> {
  private final Class<V> serializationClass;

  public HBaseValueConverter(Class<V> serializationClass) {
    this.serializationClass = serializationClass;
  }

  @Override
  public V convertInput(Object key, V value) {
    return value;
  }

  @Override
  public Object outputKey(V value) {
    return NullWritable.get();
  }

  @Override
  public V outputValue(V value) {
    return value;
  }

  @Override
  public Class<Object> getKeyClass() {
    return (Class<Object>) (Class<?>) NullWritable.class;
  }

  @Override
  public Class<V> getValueClass() {
    return serializationClass;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return false;
  }

  @Override
  public Iterable<V> convertIterableInput(Object key, Iterable<V> value) {
    return value;
  }
}
