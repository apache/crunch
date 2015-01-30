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

import com.google.common.collect.ImmutableList;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.FilterFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.lib.Cogroup;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.materialize.MaterializableMap;
import org.apache.crunch.materialize.pobject.MapPObject;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import com.google.common.collect.Lists;

public class MemTable<K, V> extends MemCollection<Pair<K, V>> implements PTable<K, V> {

  private PTableType<K, V> ptype;

  public MemTable(Iterable<Pair<K, V>> collect) {
    this(collect, null, null);
  }

  public MemTable(Iterable<Pair<K, V>> collect, PTableType<K, V> ptype, String name) {
    super(collect, ptype, name);
    this.ptype = ptype;
  }

  @Override
  public PTable<K, V> union(PTable<K, V> other) {
    return union(new PTable[] { other });
  }
  
  @Override
  public PTable<K, V> union(PTable<K, V>... others) {
    return getPipeline().unionTables(
        ImmutableList.<PTable<K, V>>builder().add(this).add(others).build());
  }

  @Override
  public PGroupedTable<K, V> groupByKey() {
    return groupByKey(null);
  }

  @Override
  public PGroupedTable<K, V> groupByKey(int numPartitions) {
    return groupByKey(null);
  }

  @Override
  public PGroupedTable<K, V> groupByKey(GroupingOptions options) {
    return new MemGroupedTable<K, V>(this, options);
  }

  @Override
  public PTable<K, V> write(Target target) {
    super.write(target);
    return this;
  }

  @Override
  public PTable<K, V> write(Target target, Target.WriteMode writeMode) {
    getPipeline().write(this, target, writeMode);
    return this;
  }

  @Override
  public PTable<K, V> cache() {
    // No-op
    return this;
  }

  @Override
  public PTable<K, V> cache(CachingOptions options) {
    // No-op
    return this;
  }

  @Override
  public PTableType<K, V> getPTableType() {
    return ptype;
  }

  @Override
  public PType<K> getKeyType() {
    if (ptype != null) {
      return ptype.getKeyType();
    }
    return null;
  }

  @Override
  public PType<V> getValueType() {
    if (ptype != null) {
      return ptype.getValueType();
    }
    return null;
  }

  @Override
  public PTable<K, V> filter(FilterFn<Pair<K, V>> filterFn) {
    return parallelDo(filterFn, getPTableType());
  }
  
  @Override
  public PTable<K, V> filter(String name, FilterFn<Pair<K, V>> filterFn) {
    return parallelDo(name, filterFn, getPTableType());
  }

  @Override
  public <U> PTable<K, U> mapValues(MapFn<V, U> mapFn, PType<U> ptype) {
    return PTables.mapValues(this, mapFn, ptype);
  }
  
  @Override
  public <U> PTable<K, U> mapValues(String name, MapFn<V, U> mapFn, PType<U> ptype) {
    return PTables.mapValues(name, this, mapFn, ptype);
  }
  
  @Override
  public <K2> PTable<K2, V> mapKeys(MapFn<K, K2> mapFn, PType<K2> ptype) {
    return PTables.mapKeys(this, mapFn, ptype);
  }
  
  @Override
  public <K2> PTable<K2, V> mapKeys(String name, MapFn<K, K2> mapFn, PType<K2> ptype) {
    return PTables.mapKeys(name, this, mapFn, ptype);
  }
  
  @Override
  public PTable<K, V> top(int count) {
    return Aggregate.top(this, count, true);
  }

  @Override
  public PTable<K, V> bottom(int count) {
    return Aggregate.top(this, count, false);
  }

  @Override
  public PTable<K, Collection<V>> collectValues() {
    return Aggregate.collectValues(this);
  }

  @Override
  public <U> PTable<K, Pair<V, U>> join(PTable<K, U> other) {
    return Join.join(this, other);
  }

  @Override
  public <U> PTable<K, Pair<Collection<V>, Collection<U>>> cogroup(PTable<K, U> other) {
    return Cogroup.cogroup(this, other);
  }

  @Override
  public PCollection<K> keys() {
    return PTables.keys(this);
  }

  @Override
  public PCollection<V> values() {
    return PTables.values(this);
  }

  @Override
  public Map<K, V> materializeToMap() {
    return new MaterializableMap<K, V>(this.materialize());
  }

  @Override
  public PObject<Map<K, V>> asMap() {
    return new MapPObject<K, V>(this);
  }

}
