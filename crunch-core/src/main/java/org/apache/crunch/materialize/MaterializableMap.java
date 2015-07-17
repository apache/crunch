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
package org.apache.crunch.materialize;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.Pair;

public class MaterializableMap<K, V> extends AbstractMap<K, V> implements Serializable {

  private transient Iterable<Pair<K, V>> iterable;
  private Map<K, V> delegate;

  public MaterializableMap(Iterable<Pair<K, V>> iterable) {
    this.iterable = iterable;
  }

  private Map<K, V> delegate() {
    if (delegate == null) {
      delegate = new HashMap<K, V>();
      for (Pair<K, V> x : iterable) {
        delegate.put(x.first(), x.second());
      }
    }
    return delegate;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return delegate().entrySet();
  }

  @Override
  public V get(Object key) {
    return delegate().get(key);
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate().containsKey(key);
  }

  @Override
  public int hashCode() {
    return delegate().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return delegate().equals(other);
  }
}