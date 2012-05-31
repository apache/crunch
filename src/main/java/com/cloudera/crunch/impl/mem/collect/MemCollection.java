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

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.FilterFn;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.fn.ExtractKeyFn;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.lib.Sample;
import com.cloudera.crunch.lib.Sort;
import com.cloudera.crunch.test.InMemoryEmitter;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class MemCollection<S> implements PCollection<S> {

  private final Collection<S> collect;
  private final PType<S> ptype;
  private String name;
  
  public MemCollection(Iterable<S> collect) {
    this(collect, null, null);
  }
  
  public MemCollection(Iterable<S> collect, PType<S> ptype) {
    this(collect, ptype, null);
  }
  
  public MemCollection(Iterable<S> collect, PType<S> ptype, String name) {
    this.collect = ImmutableList.copyOf(collect);
    this.ptype = ptype;
    this.name = name;
  }
  
  @Override
  public Pipeline getPipeline() {
    return MemPipeline.getInstance();
  }

  @Override
  public PCollection<S> union(PCollection<S>... collections) {
    Collection<S> output = Lists.newArrayList();    
    for (PCollection<S> pcollect : collections) {
      for (S s : pcollect.materialize()) {
        output.add(s);
      }
    }
    output.addAll(collect);
    return new MemCollection<S>(output, collections[0].getPType());
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<S, T> doFn, PType<T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> doFn, PType<T> type) {
    InMemoryEmitter<T> emitter = new InMemoryEmitter<T>();
    doFn.initialize();
    for (S s : collect) {
      doFn.process(s, emitter);
    }
    doFn.cleanup(emitter);
    return new MemCollection<T>(emitter.getOutput(), type, name);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(DoFn<S, Pair<K, V>> doFn, PTableType<K, V> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> doFn,
      PTableType<K, V> type) {
    InMemoryEmitter<Pair<K, V>> emitter = new InMemoryEmitter<Pair<K, V>>();
    doFn.initialize();
    for (S s : collect) {
      doFn.process(s, emitter);
    }
    doFn.cleanup(emitter);
    return new MemTable<K, V>(emitter.getOutput(), type, name);
  }

  @Override
  public PCollection<S> write(Target target) {
    getPipeline().write(this, target);
    return this;
  }

  @Override
  public Iterable<S> materialize() {
    return collect;
  }

  public Collection<S> getCollection() {
    return collect;
  }
  
  @Override
  public PType<S> getPType() {
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
  public String toString() {
    return collect.toString();
  }

  @Override
  public PTable<S, Long> count() {
	return Aggregate.count(this);
  }

  @Override
  public PCollection<S> sample(double acceptanceProbability) {
	return Sample.sample(this, acceptanceProbability);
  }

  @Override
  public PCollection<S> sample(double acceptanceProbability, long seed) {
	return Sample.sample(this, seed, acceptanceProbability);
  }

  @Override
  public PCollection<S> max() {
	return Aggregate.max(this);
  }

  @Override
  public PCollection<S> min() {
	return Aggregate.min(this);
  }

  @Override
  public PCollection<S> sort(boolean ascending) {
	return Sort.sort(this, ascending ? Sort.Order.ASCENDING : Sort.Order.DESCENDING);
  }

  @Override
  public PCollection<S> filter(FilterFn<S> filterFn) {
    return parallelDo(filterFn, getPType());
  }
  
  @Override
  public PCollection<S> filter(String name, FilterFn<S> filterFn) {
    return parallelDo(name, filterFn, getPType());
  }
  
  @Override
  public <K> PTable<K, S> by(MapFn<S, K> mapFn, PType<K> keyType) {
    return parallelDo(new ExtractKeyFn<K, S>(mapFn), getTypeFamily().tableOf(keyType, getPType()));
  }

  @Override
  public <K> PTable<K, S> by(String name, MapFn<S, K> mapFn, PType<K> keyType) {
    return parallelDo(name, new ExtractKeyFn<K, S>(mapFn), getTypeFamily().tableOf(keyType, getPType()));
  }
}
