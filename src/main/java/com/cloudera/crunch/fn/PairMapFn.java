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
package com.cloudera.crunch.fn;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;

public class PairMapFn<K, V, S, T> extends MapFn<Pair<K, V>, Pair<S, T>> {
  private MapFn<K, S> keys;
  private MapFn<V, T> values;

  public PairMapFn(MapFn<K, S> keys, MapFn<V, T> values) {
    this.keys = keys;
    this.values = values;
  }

  public void initialize() {
    keys.initialize();
    values.initialize();
  }

  @Override
  public Pair<S, T> map(Pair<K, V> input) {
    return Pair.of(keys.map(input.first()), values.map(input.second()));
  }
}