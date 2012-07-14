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
package org.apache.crunch.impl.mem.collect;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.crunch.CombineFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class MemGroupedTable<K, V> extends MemCollection<Pair<K, Iterable<V>>> implements PGroupedTable<K, V> {

  private final MemTable<K, V> parent;

  private static <S, T> Map<S, Collection<T>> createMapFor(PType<S> keyType, GroupingOptions options, Pipeline pipeline) {
    if (options != null && options.getSortComparatorClass() != null) {
      RawComparator<S> rc = ReflectionUtils.newInstance(options.getSortComparatorClass(), pipeline.getConfiguration());
      return new TreeMap<S, Collection<T>>(rc);
    } else if (keyType != null && Comparable.class.isAssignableFrom(keyType.getTypeClass())) {
      return new TreeMap<S, Collection<T>>();
    }
    return Maps.newHashMap();
  }

  private static <S, T> Iterable<Pair<S, Iterable<T>>> buildMap(MemTable<S, T> parent, GroupingOptions options) {
    PType<S> keyType = parent.getKeyType();
    Map<S, Collection<T>> map = createMapFor(keyType, options, parent.getPipeline());

    for (Pair<S, T> pair : parent.materialize()) {
      S key = pair.first();
      if (!map.containsKey(key)) {
        map.put(key, Lists.<T> newArrayList());
      }
      map.get(key).add(pair.second());
    }

    List<Pair<S, Iterable<T>>> values = Lists.newArrayList();
    for (Map.Entry<S, Collection<T>> e : map.entrySet()) {
      values.add(Pair.of(e.getKey(), (Iterable<T>) e.getValue()));
    }
    return values;
  }

  public MemGroupedTable(MemTable<K, V> parent, GroupingOptions options) {
    super(buildMap(parent, options));
    this.parent = parent;
  }

  @Override
  public PCollection<Pair<K, Iterable<V>>> union(PCollection<Pair<K, Iterable<V>>>... collections) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PCollection<Pair<K, Iterable<V>>> write(Target target) {
    getPipeline().write(this.ungroup(), target);
    return this;
  }

  @Override
  public PType<Pair<K, Iterable<V>>> getPType() {
    PTableType<K, V> parentType = parent.getPTableType();
    if (parentType != null) {
      return parentType.getGroupedTableType();
    }
    return null;
  }

  @Override
  public PTypeFamily getTypeFamily() {
    return parent.getTypeFamily();
  }

  @Override
  public long getSize() {
    return parent.getSize();
  }

  @Override
  public String getName() {
    return "MemGrouped(" + parent.getName() + ")";
  }

  @Override
  public PTable<K, V> combineValues(CombineFn<K, V> combineFn) {
    return parallelDo(combineFn, parent.getPTableType());
  }

  @Override
  public PTable<K, V> ungroup() {
    return parent;
  }
}
