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
package org.apache.crunch.impl.mr.collect;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.DoFn;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Pipeline;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.fn.ExtractKeyFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.materialize.pobject.CollectionPObject;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class PCollectionImpl<S> implements PCollection<S> {

  private static final Log LOG = LogFactory.getLog(PCollectionImpl.class);

  private final String name;
  protected MRPipeline pipeline;
  private SourceTarget<S> materializedAt;
  private final ParallelDoOptions options;
  
  public PCollectionImpl(String name) {
    this(name, ParallelDoOptions.builder().build());
  }
  
  public PCollectionImpl(String name, ParallelDoOptions options) {
    this.name = name;
    this.options = options;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public PCollection<S> union(PCollection<S>... collections) {
    List<PCollectionImpl<S>> internal = Lists.newArrayList();
    internal.add(this);
    for (PCollection<S> collection : collections) {
      internal.add((PCollectionImpl<S>) collection);
    }
    return new UnionCollection<S>(internal);
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<S, T> fn, PType<T> type) {
    MRPipeline pipeline = (MRPipeline) getPipeline();
    return parallelDo("S" + pipeline.getNextAnonymousStageId(), fn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> fn, PType<T> type) {
    return new DoCollectionImpl<T>(name, getChainingCollection(), fn, type);
  }
  
  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> fn, PType<T> type,
      ParallelDoOptions options) {
    return new DoCollectionImpl<T>(name, getChainingCollection(), fn, type, options);
  }
  
  @Override
  public <K, V> PTable<K, V> parallelDo(DoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    MRPipeline pipeline = (MRPipeline) getPipeline();
    return parallelDo("S" + pipeline.getNextAnonymousStageId(), fn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return new DoTableImpl<K, V>(name, getChainingCollection(), fn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> fn, PTableType<K, V> type,
      ParallelDoOptions options) {
    return new DoTableImpl<K, V>(name, getChainingCollection(), fn, type, options);
  }

  public PCollection<S> write(Target target) {
    if (materializedAt != null) {
      getPipeline().write(new InputCollection<S>(materializedAt, (MRPipeline) getPipeline()), target);
    } else {
      getPipeline().write(this, target);
    }
    return this;
  }

  @Override
  public Iterable<S> materialize() {
    if (getSize() == 0) {
      LOG.warn("Materializing an empty PCollection: " + this.getName());
      return Collections.emptyList();
    }
    return getPipeline().materialize(this);
  }

  /** {@inheritDoc} */
  @Override
  public PObject<Collection<S>> asCollection() {
    return new CollectionPObject<S>(this);
  }

  public SourceTarget<S> getMaterializedAt() {
    return materializedAt;
  }

  public void materializeAt(SourceTarget<S> sourceTarget) {
    this.materializedAt = sourceTarget;
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

  @Override
  public PTable<S, Long> count() {
    return Aggregate.count(this);
  }

  @Override
  public PObject<Long> length() {
    return Aggregate.length(this);
  }

  @Override
  public PObject<S> max() {
    return Aggregate.max(this);
  }

  @Override
  public PObject<S> min() {
    return Aggregate.min(this);
  }

  @Override
  public PTypeFamily getTypeFamily() {
    return getPType().getFamily();
  }

  public abstract DoNode createDoNode();

  public abstract List<PCollectionImpl<?>> getParents();

  public PCollectionImpl<?> getOnlyParent() {
    List<PCollectionImpl<?>> parents = getParents();
    if (parents.size() != 1) {
      throw new IllegalArgumentException("Expected exactly one parent PCollection");
    }
    return parents.get(0);
  }

  @Override
  public Pipeline getPipeline() {
    if (pipeline == null) {
      pipeline = (MRPipeline) getParents().get(0).getPipeline();
    }
    return pipeline;
  }
  
  public Set<SourceTarget<?>> getTargetDependencies() {
    Set<SourceTarget<?>> targetDeps = options.getSourceTargets();
    for (PCollectionImpl<?> parent : getParents()) {
      targetDeps = Sets.union(targetDeps, parent.getTargetDependencies());
    }
    return targetDeps;
  }
  
  public int getDepth() {
    int parentMax = 0;
    for (PCollectionImpl parent : getParents()) {
      parentMax = Math.max(parent.getDepth(), parentMax);
    }
    return 1 + parentMax;
  }

  public interface Visitor {
    void visitInputCollection(InputCollection<?> collection);

    void visitUnionCollection(UnionCollection<?> collection);

    void visitDoFnCollection(DoCollectionImpl<?> collection);

    void visitDoTable(DoTableImpl<?, ?> collection);

    void visitGroupedTable(PGroupedTableImpl<?, ?> collection);
  }

  public void accept(Visitor visitor) {
    if (materializedAt != null) {
      visitor.visitInputCollection(new InputCollection<S>(materializedAt, (MRPipeline) getPipeline()));
    } else {
      acceptInternal(visitor);
    }
  }

  protected abstract void acceptInternal(Visitor visitor);

  @Override
  public long getSize() {
    if (materializedAt != null) {
      long sz = materializedAt.getSize(getPipeline().getConfiguration());
      if (sz > 0) {
        return sz;
      }
    }
    return getSizeInternal();
  }

  protected abstract long getSizeInternal();
  
  /**
   * Retrieve the PCollectionImpl to be used for chaining within PCollectionImpls further down the pipeline.
   * @return The PCollectionImpl instance to be chained
   */
  protected PCollectionImpl<S> getChainingCollection(){
    return this;
  }
  
}
