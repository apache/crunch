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
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.DelegatingReadableData;

import java.util.List;

public class BaseDoTable<K, V> extends PTableBase<K, V> implements PTable<K, V> {

  private final PCollectionImpl<?> parent;
  protected final DoFn<?, Pair<K, V>> combineFn;
  protected final DoFn<?, Pair<K, V>> fn;
  protected final PTableType<K, V> type;

  private static <S, K, V> CombineFn<K, V> asCombineFn(final DoFn<S, Pair<K, V>> fn) {
    if (fn instanceof CombineFn) {
      return (CombineFn) fn;
    }
    return null;
  }

  protected <S> BaseDoTable(String name, PCollectionImpl<S> parent, DoFn<S, Pair<K, V>> fn, PTableType<K, V> ntype,
                            ParallelDoOptions options) {
    this(name, parent, asCombineFn(fn), fn, ntype, options);
  }

  protected <S> BaseDoTable(
      String name,
      PCollectionImpl<S> parent,
      CombineFn<K, V> combineFn,
      DoFn<S, Pair<K, V>> fn,
      PTableType<K, V> ntype) {
    this(name, parent, combineFn, fn, ntype, ParallelDoOptions.builder().build());
  }

  protected <S> BaseDoTable(
      String name,
      PCollectionImpl<S> parent,
      CombineFn<K, V> combineFn,
      DoFn<S, Pair<K, V>> fn,
      PTableType<K, V> ntype,
      ParallelDoOptions options) {
    super(name, parent.getPipeline(), options);
    this.parent = parent;
    this.combineFn = combineFn;
    this.fn = fn;
    this.type = ntype;
  }

  @Override
  protected long getSizeInternal() {
    long parentSize = parent.getSize();
    if (parentSize == 0L) {
      return parentSize;
    }
    return Math.max(1L, (long) (fn.scaleFactor() * parentSize));
  }

  @Override
  public PTableType<K, V> getPTableType() {
    return type;
  }

  @Override
  protected ReadableData<Pair<K, V>> getReadableDataInternal() {
    if (getOnlyParent() instanceof BaseGroupedTable) {
      return materializedData();
    }
    return new DelegatingReadableData(getOnlyParent().asReadable(false), fn);
  }

  @Override
  public PType<Pair<K, V>> getPType() {
    return type;
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
    visitor.visitDoTable(this);
  }
}
