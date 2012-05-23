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
package com.cloudera.crunch.impl.mem.collect;

import java.util.Collection;
import java.util.List;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.lib.Cogroup;
import com.cloudera.crunch.lib.Join;
import com.cloudera.crunch.lib.PTables;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
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
  public PTable<K, V> union(PTable<K, V>... others) {
    List<Pair<K, V>> values = Lists.newArrayList();
    values.addAll(getCollection());
    for (PTable<K, V> ptable : others) {
      for (Pair<K, V> p : ptable.materialize()) {
        values.add(p);
      }
    }
    return new MemTable<K, V>(values, others[0].getPTableType(), null);
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

}
