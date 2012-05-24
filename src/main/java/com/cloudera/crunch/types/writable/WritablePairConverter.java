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

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.Converter;

public class WritablePairConverter<K, V> implements Converter<K, V, Pair<K, V>, Pair<K, Iterable<V>>> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  
  public WritablePairConverter(Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }
  
  @Override
  public Pair<K, V> convertInput(K key, V value) {
    return Pair.of(key, value);
  }

  @Override
  public K outputKey(Pair<K, V> value) {
    return value.first();
  }

  @Override
  public V outputValue(Pair<K, V> value) {
    return value.second();
  }

  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }

  @Override
  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public Pair<K, Iterable<V>> convertIterableInput(K key, Iterable<V> value) {
    return Pair.of(key, value);
  }
}
