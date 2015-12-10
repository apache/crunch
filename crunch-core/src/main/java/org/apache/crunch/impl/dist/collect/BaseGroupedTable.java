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
package org.apache.crunch.impl.dist.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.crunch.Aggregator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.IMapFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.fn.IFnHelpers;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PType;

import java.util.List;
import java.util.Set;

public class BaseGroupedTable<K, V> extends PCollectionImpl<Pair<K, Iterable<V>>>
    implements PGroupedTable<K, V> {

  protected final PTableBase<K, V> parent;
  protected final GroupingOptions groupingOptions;
  protected final PGroupedTableType<K, V> ptype;

  protected BaseGroupedTable(PTableBase<K, V> parent) {
    this(parent, null);
  }

  protected BaseGroupedTable(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    super("GBK", parent.getPipeline());
    this.parent = parent;
    this.groupingOptions = groupingOptions;
    this.ptype = parent.getPTableType().getGroupedTableType();
  }

  @Override
  protected ReadableData<Pair<K, Iterable<V>>> getReadableDataInternal() {
    throw new UnsupportedOperationException("PGroupedTable does not currently support readability");
  }

  @Override
  protected long getSizeInternal() {
    return parent.getSizeInternal();
  }

  @Override
  public PType<Pair<K, Iterable<V>>> getPType() {
    return ptype;
  }

  @Override
  public PTable<K, V> combineValues(CombineFn<K, V> combineFn, CombineFn<K, V> reduceFn) {
      return pipeline.getFactory().createDoTable(
          "combine",
          getChainingCollection(),
          combineFn,
          reduceFn,
          parent.getPTableType());
  }

  @Override
  public PTable<K, V> combineValues(CombineFn<K, V> combineFn) {
    return combineValues(combineFn, combineFn);
  }

  @Override
  public PTable<K, V> combineValues(Aggregator<V> agg) {
    return combineValues(Aggregators.<K, V>toCombineFn(agg, parent.getValueType()));
  }

  @Override
  public PTable<K, V> combineValues(Aggregator<V> combineAgg, Aggregator<V> reduceAgg) {
    return combineValues(Aggregators.<K, V>toCombineFn(combineAgg, parent.getValueType()),
        Aggregators.<K, V>toCombineFn(reduceAgg, parent.getValueType()));
  }

  private static class Ungroup<K, V> extends DoFn<Pair<K, Iterable<V>>, Pair<K, V>> {
    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      for (V v : input.second()) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }
  }

  @Override
  public PTable<K, V> ungroup() {
    return parallelDo("ungroup", new Ungroup<K, V>(), parent.getPTableType());
  }

  @Override
  public <U> PTable<K, U> mapValues(MapFn<Iterable<V>, U> mapFn, PType<U> ptype) {
    return PTables.mapValues(this, mapFn, ptype);
  }

  @Override
  public <U> PTable<K, U> mapValues(IMapFn<Iterable<V>, U> mapFn, PType<U> ptype) {
    return PTables.mapValues(this, IFnHelpers.wrapMap(mapFn), ptype);
  }


  @Override
  public <U> PTable<K, U> mapValues(String name, MapFn<Iterable<V>, U> mapFn, PType<U> ptype) {
    return PTables.mapValues(name, this, mapFn, ptype);
  }

  @Override
  public PGroupedTableType<K, V> getGroupedTableType() {
    return ptype;
  }

  @Override
  public Set<Target> getTargetDependencies() {
    Set<Target> td = Sets.newHashSet(super.getTargetDependencies());
    if (groupingOptions != null) {
      td.addAll(groupingOptions.getSourceTargets());
    }
    return ImmutableSet.copyOf(td);
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.<PCollectionImpl<?>> of(parent);
  }

  @Override
  public long getLastModifiedAt() {
    return parent.getLastModifiedAt();
  }

  @Override
  protected void acceptInternal(Visitor visitor) {
    visitor.visitGroupedTable(this);
  }


  @Override
  protected PCollectionImpl<Pair<K, Iterable<V>>> getChainingCollection() {
    // Use a copy for chaining to allow sending the output of a single grouped table to multiple outputs
    // TODO This should be implemented in a cleaner way in the planner
    return pipeline.getFactory().createGroupedTable(parent, groupingOptions);
  }
}
