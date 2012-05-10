/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.fn;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;

/**
 * Wrapper function for converting a {@code MapFn} into a key-value pair that
 * is used to convert from a {@code PCollection<V>} to a {@code PTable<K, V>}.
 */
public class ExtractKeyFn<K, V> extends MapFn<V, Pair<K, V>> {

  private final MapFn<V, K> mapFn;
  
  public ExtractKeyFn(MapFn<V, K> mapFn) {
    this.mapFn = mapFn;
  }
  
  @Override
  public void initialize() {
    this.mapFn.setContext(getContext());
  }
  
  @Override
  public Pair<K, V> map(V input) {
    return Pair.of(mapFn.map(input), input);
  }
}
