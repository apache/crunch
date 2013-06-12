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

import java.util.List;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import com.google.common.collect.ImmutableList;

public class DoTableImpl<K, V> extends PTableBase<K, V> implements PTable<K, V> {

  private final PCollectionImpl<?> parent;
  private final DoFn<?, Pair<K, V>> fn;
  private final PTableType<K, V> type;

  <S> DoTableImpl(String name, PCollectionImpl<S> parent, DoFn<S, Pair<K, V>> fn, PTableType<K, V> ntype) {
    this(name, parent, fn, ntype, ParallelDoOptions.builder().build());
  }
  
  <S> DoTableImpl(String name, PCollectionImpl<S> parent, DoFn<S, Pair<K, V>> fn, PTableType<K, V> ntype,
      ParallelDoOptions options) {
    super(name, options);
    this.parent = parent;
    this.fn = fn;
    this.type = ntype;
  }

  @Override
  protected long getSizeInternal() {
    return (long) (fn.scaleFactor() * parent.getSize());
  }

  @Override
  public PTableType<K, V> getPTableType() {
    return type;
  }

  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitDoTable(this);
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
  public DoNode createDoNode() {
    return DoNode.createFnNode(getName(), fn, type);
  }

  public boolean hasCombineFn() {
    return fn instanceof CombineFn;
  }
  
  @Override
  public long getLastModifiedAt() {
    return parent.getLastModifiedAt();
  }
}
