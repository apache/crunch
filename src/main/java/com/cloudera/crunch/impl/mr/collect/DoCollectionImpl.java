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

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.ImmutableList;

public class DoCollectionImpl<S> extends PCollectionImpl<S> {

  private final PCollectionImpl<Object> parent;
  private final DoFn<Object, S> fn;
  private final PType<S> ntype;

  <T> DoCollectionImpl(String name, PCollectionImpl<T> parent, DoFn<T, S> fn,
      PType<S> ntype) {
    super(name);
    this.parent = (PCollectionImpl<Object>) parent;
    this.fn = (DoFn<Object, S>) fn;
    this.ntype = ntype;
  }

  @Override
  protected long getSizeInternal() {
    return (long) (fn.scaleFactor() * parent.getSize());
  }
  
  @Override
  public PType<S> getPType() {
    return ntype;
  }

  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitDoFnCollection(this);
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.<PCollectionImpl<?>> of(parent);
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createFnNode(getName(), fn, ntype);
  }
}
