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
package com.cloudera.crunch.impl.mr.collect;

import java.util.List;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.ImmutableList;

public class DoTableImpl<K, V> extends PTableBase<K, V> implements
    PTable<K, V> {

  private final PCollectionImpl<?> parent;
  private final DoFn<?, Pair<K, V>> fn;
  private final PTableType<K, V> type;

  <S> DoTableImpl(String name, PCollectionImpl<S> parent,
      DoFn<S, Pair<K, V>> fn, PTableType<K, V> ntype) {
    super(name);
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
}
