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
package com.cloudera.crunch.impl.mem.collect;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
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
import com.google.common.collect.Multimap;

public class MemGroupedTable<K, V> implements PGroupedTable<K, V> {

  private final MemTable<K, V> parent;
  private final Map<K, Collection<V>> values;
  
  public MemGroupedTable(MemTable<K, V> parent) {
    this.parent = parent;
    this.values = parent.getMultimap().asMap();
  }
  
  @Override
  public Pipeline getPipeline() {
    return MemPipeline.getInstance();
  }

  @Override
  public PCollection<Pair<K, Iterable<V>>> union(
      PCollection<Pair<K, Iterable<V>>>... collections) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<Pair<K, Iterable<V>>, T> doFn,
      PType<T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name,
      DoFn<Pair<K, Iterable<V>>, T> doFn, PType<T> type) {
    CollectEmitter<T> emitter = new CollectEmitter<T>();
    doFn.initialize();
    for (Map.Entry<K, Collection<V>> e : values.entrySet()) {
      doFn.process(Pair.of(e.getKey(), (Iterable<V>) e.getValue()), emitter);
    }
    doFn.cleanup(emitter);
    return new MemCollection<T>(emitter.getOutput(), type, name);
  }

  @Override
  public <S, T> PTable<S, T> parallelDo(DoFn<Pair<K, Iterable<V>>, Pair<S, T>> doFn, PTableType<S, T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <S, T> PTable<S, T> parallelDo(String name,
      DoFn<Pair<K, Iterable<V>>, Pair<S, T>> doFn, PTableType<S, T> type) {
    MultimapEmitter<S, T> emitter = new MultimapEmitter<S, T>();
    doFn.initialize();
    for (Map.Entry<K, Collection<V>> e : values.entrySet()) {
      doFn.process(Pair.of(e.getKey(), (Iterable<V>) e.getValue()), emitter);
    }
    doFn.cleanup(emitter);
    return new MemTable<S, T>(emitter.getOutput(), type, name);
  }

  @Override
  public PCollection<Pair<K, Iterable<V>>> write(Target target) {
    getPipeline().write(this, target);
    return this;
  }

  @Override
  public Iterable<Pair<K, Iterable<V>>> materialize() {
    return new Iterable<Pair<K, Iterable<V>>>() {
      @Override
      public Iterator<Pair<K, Iterable<V>>> iterator() {
        return new Iterator<Pair<K, Iterable<V>>>() {
          Iterator<Map.Entry<K, Collection<V>>> iter = values.entrySet().iterator();
          
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public Pair<K, Iterable<V>> next() {
            Map.Entry<K, Collection<V>> e = iter.next();
            return Pair.of(e.getKey(), (Iterable<V>) e.getValue());
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
  public PType<Pair<K, Iterable<V>>> getPType() {
    //TODO hrm?
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
    return "MemGrouped";
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
