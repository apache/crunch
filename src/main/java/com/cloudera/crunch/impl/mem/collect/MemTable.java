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

import java.util.Iterator;
import java.util.Map;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class MemTable<K, V> implements PTable<K, V> {

  private final Multimap<K, V> collect;
  private PTableType<K, V> ptype;
  private String name;
  
  public MemTable(Multimap<K, V> collect) {
    this(collect, null, null);
  }
  
  public MemTable(Multimap<K, V> collect, PTableType<K, V> ptype, String name) {
    this.collect = ImmutableMultimap.copyOf(collect);
    this.ptype = ptype;
    this.name = name;
  }
  
  public Multimap<K, V> getMultimap() {
    return collect;
  }
  
  @Override
  public Pipeline getPipeline() {
    return MemPipeline.getInstance();
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<Pair<K, V>, T> doFn, PType<T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<Pair<K, V>, T> doFn, PType<T> type) {
    CollectEmitter<T> emitter = new CollectEmitter<T>();
    doFn.initialize();
    for (Map.Entry<K, V> kv : collect.entries()) {
      doFn.process(Pair.of(kv.getKey(), kv.getValue()), emitter);
    }
    doFn.cleanup(emitter);
    return new MemCollection<T>(emitter.getOutput(), type, name);
  }

  @Override
  public <S, T> PTable<S, T> parallelDo(DoFn<Pair<K, V>, Pair<S, T>> doFn,
      PTableType<S, T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <S, T> PTable<S, T> parallelDo(String name, DoFn<Pair<K, V>, Pair<S, T>> doFn, PTableType<S, T> type) {
    MultimapEmitter<S, T> emitter = new MultimapEmitter<S, T>();
    doFn.initialize();
    for (Map.Entry<K, V> kv : collect.entries()) {
      doFn.process(Pair.of(kv.getKey(), kv.getValue()), emitter);
    }
    doFn.cleanup(emitter);
    return new MemTable<S, T>(emitter.getOutput(), type, name);
  }

  @Override
  public Iterable<Pair<K, V>> materialize() {
    return new Iterable<Pair<K, V>>() {
      @Override
      public Iterator<Pair<K, V>> iterator() {
        return new Iterator<Pair<K, V>>() {
          Iterator<Map.Entry<K, V>> iter = collect.entries().iterator();
          
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public Pair<K, V> next() {
            Map.Entry<K, V> e = iter.next();
            return Pair.of(e.getKey(), e.getValue());
          }

          @Override
          public void remove() {
            iter.remove();
          }
        };
      }
    };
  }

  @Override
  public PType<Pair<K, V>> getPType() {
    return ptype;
  }

  @Override
  public PTypeFamily getTypeFamily() {
    if (ptype != null) {
      return ptype.getFamily();
    }
    return null;
  }

  @Override
  public long getSize() {
    return collect.size();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public PTable<K, V> union(PTable<K, V>... others) {
    Multimap<K, V> values = HashMultimap.create();
    for (PTable<K, V> ptable : others) {
      for (Pair<K, V> p : ptable.materialize()) {
        values.put(p.first(), p.second());
      }
    }
    return new MemTable<K, V>(values);
  }

  @Override
  public PCollection<Pair<K, V>> union(PCollection<Pair<K, V>>... collections) {
    Multimap<K, V> values = HashMultimap.create();
    for (PCollection<Pair<K, V>> ptable : collections) {
      for (Pair<K, V> p : ptable.materialize()) {
        values.put(p.first(), p.second());
      }
    }
    return new MemTable<K, V>(values);
  }

  @Override
  public PGroupedTable<K, V> groupByKey() {
    return new MemGroupedTable<K, V>(this);
  }

  @Override
  public PGroupedTable<K, V> groupByKey(int numPartitions) {
    return groupByKey();
  }

  @Override
  public PGroupedTable<K, V> groupByKey(GroupingOptions options) {
    //TODO: make this work w/the grouping options
    return groupByKey();
  }

  @Override
  public PTable<K, V> write(Target target) {
    getPipeline().write(this, target);
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
}
