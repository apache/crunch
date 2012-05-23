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

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.ImmutableList;

public class UnionCollection<S> extends PCollectionImpl<S> {

  private List<PCollectionImpl<S>> parents;
  private long size = 0;
  
  private static <S> String flatName(List<PCollectionImpl<S>> collections) {
    StringBuilder sb = new StringBuilder("union(");
    for (int i = 0; i < collections.size(); i++) {
      if (i != 0) {
        sb.append(',');
      }
      sb.append(collections.get(i).getName());
    }
    return sb.append(')').toString();
  }
  
  UnionCollection(List<PCollectionImpl<S>> collections) {
    super(flatName(collections));
    this.parents = ImmutableList.copyOf(collections);
    this.pipeline = (MRPipeline) parents.get(0).getPipeline();
    for (PCollectionImpl<S> parent : parents) {
      if (this.pipeline != parent.getPipeline()) {
        throw new IllegalStateException(
            "Cannot union PCollections from different Pipeline instances");
      }
      size += parent.getSize();
    }
  }

  @Override
  protected long getSizeInternal() {
    return size;
  }
  
  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitUnionCollection(this);
  }

  @Override
  public PType<S> getPType() {
    return parents.get(0).getPType();
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.<PCollectionImpl<?>> copyOf(parents);
  }

  @Override
  public DoNode createDoNode() {
    throw new UnsupportedOperationException(
        "Unioned collection does not support DoNodes");
  }
}
