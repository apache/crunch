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
package com.cloudera.crunch.type;

import com.cloudera.crunch.Pair;

public class PairConverter<K, V> implements Converter<K, V, Pair<K, V>> {
  public K outputKey(Pair<K, V> input) {
    return input.first();
  }

  public V outputValue(Pair<K, V> input) {
    return input.second();
  }

  public Pair<K, V> convertInput(K key, V value) {
    return Pair.of(key, value);
  }
}
