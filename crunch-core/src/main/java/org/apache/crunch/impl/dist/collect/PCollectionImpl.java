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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.crunch.Aggregator;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.PipelineCallable;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.fn.ExtractKeyFn;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.materialize.pobject.CollectionPObject;
import org.apache.crunch.materialize.pobject.FirstElementPObject;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class PCollectionImpl<S> implements PCollection<S> {

  private final String name;
  protected DistributedPipeline pipeline;
  private boolean materialized;
  protected SourceTarget<S> materializedAt;
  protected final ParallelDoOptions doOptions;
  private long size = -1L;
  private boolean breakpoint;

  public PCollectionImpl(String name, DistributedPipeline pipeline) {
    this(name, pipeline, ParallelDoOptions.builder().build());
  }
  
  public PCollectionImpl(String name, DistributedPipeline pipeline, ParallelDoOptions doOptions) {
    this.name = name;
    this.pipeline = pipeline;
    this.doOptions = doOptions;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DistributedPipeline getPipeline() {
    return pipeline;
  }

  public ParallelDoOptions getParallelDoOptions() {
    return doOptions;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public Iterable<S> materialize() {
    if (!waitingOnTargets() && getSize() == 0) {
      System.err.println("Materializing an empty PCollection: " + this.getName());
      return Collections.emptyList();
    }
    if (materializedAt != null && (materializedAt instanceof ReadableSource)) {
      try {
        return ((ReadableSource<S>) materializedAt).read(getPipeline().getConfiguration());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error reading materialized data", e);
      }
    }
    materialized = true;
    return pipeline.materialize(this);
  }

  @Override
  public PCollection<S> cache() {
    return cache(CachingOptions.DEFAULT);
  }

  @Override
  public PCollection<S> cache(CachingOptions options) {
    pipeline.cache(this, options);
    return this;
  }

  @Override
  public PCollection<S> union(PCollection<S> other) {
    return union(new PCollection[] { other });
  }
  
  @Override
  public PCollection<S> union(PCollection<S>... collections) {
    List<PCollectionImpl<S>> internal = Lists.newArrayList();
    internal.add(this);
    for (PCollection<S> collection : collections) {
      internal.add((PCollectionImpl<S>) collection.parallelDo(IdentityFn.<S>getInstance(), collection.getPType()));
    }
    return pipeline.getFactory().createUnionCollection(internal);
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<S, T> fn, PType<T> type) {
    return parallelDo("S" + pipeline.getNextAnonymousStageId(), fn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> fn, PType<T> type) {
    return parallelDo(name, fn, type, ParallelDoOptions.builder().build());
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> fn, PType<T> type,
      ParallelDoOptions options) {
    return pipeline.getFactory().createDoCollection(name, getChainingCollection(), fn, type, options);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(DoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo("S" + pipeline.getNextAnonymousStageId(), fn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(name, fn, type, ParallelDoOptions.builder().build());
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> fn, PTableType<K, V> type,
      ParallelDoOptions options) {
    return pipeline.getFactory().createDoTable(name, getChainingCollection(), fn, type, options);
  }


  public PCollection<S> write(Target target) {
    if (materializedAt != null) {
      getPipeline().write(
          pipeline.getFactory().createInputCollection(materializedAt, getName(), pipeline, doOptions),
          target);
    } else {
      getPipeline().write(this, target);
    }
    return this;
  }

  @Override
  public PCollection<S> write(Target target, Target.WriteMode writeMode) {
    if (materializedAt != null) {
      getPipeline().write(
          pipeline.getFactory().createInputCollection(materializedAt, getName(), pipeline, doOptions),
          target,
          writeMode);
    } else {
      getPipeline().write(this, target, writeMode);
    }
    return this;
  }

  public interface Visitor {
    void visitInputCollection(BaseInputCollection<?> collection);

    void visitUnionCollection(BaseUnionCollection<?> collection);

    void visitDoCollection(BaseDoCollection<?> collection);

    void visitDoTable(BaseDoTable<?, ?> collection);

    void visitGroupedTable(BaseGroupedTable<?, ?> collection);
  }

  public void accept(Visitor visitor) {
    if (materializedAt != null) {
      visitor.visitInputCollection(
              pipeline.getFactory().createInputCollection(materializedAt, getName(), pipeline, doOptions));
    } else {
      acceptInternal(visitor);
    }
  }

  protected boolean waitingOnTargets() {
    for (PCollectionImpl parent : getParents()) {
      if (parent.waitingOnTargets()) {
        return true;
      }
    }
    return false;
  }

  protected abstract void acceptInternal(Visitor visitor);

  public void setBreakpoint() {
    this.breakpoint = true;
  }

  public boolean isBreakpoint() {
    return breakpoint;
  }

  /** {@inheritDoc} */
  @Override
  public PObject<Collection<S>> asCollection() {
    return new CollectionPObject<S>(this);
  }

  @Override
  public PObject<S> first() { return new FirstElementPObject<S>(this); }

  @Override
  public <Output> Output sequentialDo(String label, PipelineCallable<Output> pipelineCallable) {
    pipelineCallable.dependsOn(label, this);
    return getPipeline().sequentialDo(pipelineCallable);
  }

  public SourceTarget<S> getMaterializedAt() {
    return materializedAt;
  }

  public void materializeAt(SourceTarget<S> sourceTarget) {
    this.materializedAt = sourceTarget;
    this.size = materializedAt.getSize(getPipeline().getConfiguration());
  }

  @Override
  public PCollection<S> filter(FilterFn<S> filterFn) {
    return parallelDo("Filter with " + filterFn.getClass().getSimpleName(), filterFn, getPType());
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
  public PCollection<S> aggregate(Aggregator<S> aggregator) {
    return Aggregate.aggregate(this, aggregator);
  }

  @Override
  public PTypeFamily getTypeFamily() {
    return getPType().getFamily();
  }

  public abstract List<PCollectionImpl<?>> getParents();

  public PCollectionImpl<?> getOnlyParent() {
    List<PCollectionImpl<?>> parents = getParents();
    if (parents.size() != 1) {
      throw new IllegalArgumentException("Expected exactly one parent PCollection");
    }
    return parents.get(0);
  }

  public Set<Target> getTargetDependencies() {
    Set<Target> targetDeps = Sets.<Target>newHashSet(doOptions.getTargets());
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

  @Override
  public ReadableData<S> asReadable(boolean materialize) {
    if (materializedAt != null && (materializedAt instanceof ReadableSource)) {
      return ((ReadableSource) materializedAt).asReadable();
    } else if (materialized || materialize) {
      return pipeline.getMaterializeSourceTarget(this).asReadable();
    } else {
      return getReadableDataInternal();
    }
  }

  protected ReadableData<S> materializedData() {
    materialized = true;
    return pipeline.getMaterializeSourceTarget(this).asReadable();
  }

  protected abstract ReadableData<S> getReadableDataInternal();

  @Override
  public long getSize() {
    if (size < 0) {
      this.size = getSizeInternal();
    }
    return size;
  }

  protected abstract long getSizeInternal();

  /**
  * The time of the most recent modification to one of the input sources to the collection.  If the time can
  * not be determined then {@code -1} should be returned.
  * @return time of the most recent modification to one of the input sources to the collection.
  */
  public abstract long getLastModifiedAt();
  
  /**
   * Retrieve the PCollectionImpl to be used for chaining within PCollectionImpls further down the pipeline.
   * @return The PCollectionImpl instance to be chained
   */
  protected PCollectionImpl<S> getChainingCollection() {
    return this;
  }
  
}
