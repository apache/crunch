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
package com.cloudera.crunch.materialize;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.cloudera.crunch.Pair;

public class MaterializableMap<K, V> extends AbstractMap<K, V> {
  
  private Iterable<Pair<K, V>> iterable;
  private Set<Map.Entry<K, V>> entrySet;
  
  public MaterializableMap(Iterable<Pair<K, V>> iterable) {
  	this.iterable = iterable;
  }
  
  private Set<Map.Entry<K, V>> toMapEntries(Iterable<Pair<K, V>> xs) {
    HashMap<K, V> m = new HashMap<K, V>();
    for (Pair<K, V> x : xs)
      m.put(x.first(), x.second());
    return m.entrySet();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    if (entrySet == null)
      entrySet = toMapEntries(iterable);
    return entrySet;
  }

}