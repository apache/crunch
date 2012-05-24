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

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class UnionTable<K, V> extends PTableBase<K, V> {

  private PTableType<K, V> ptype;
  private List<PCollectionImpl<Pair<K, V>>> parents;
  private long size;
  
  private static <K, V> String flatName(List<PTableBase<K, V>> tables) {
    StringBuilder sb = new StringBuilder("union(");
    for (int i = 0; i < tables.size(); i++) {
      if (i != 0) {
        sb.append(',');
      }
      sb.append(tables.get(i).getName());
    }
    return sb.append(')').toString();
  }
  
  public UnionTable(List<PTableBase<K, V>> tables) {
    super(flatName(tables));
    this.ptype = tables.get(0).getPTableType();
    this.pipeline = (MRPipeline) tables.get(0).getPipeline();
    this.parents = Lists.newArrayList();
    for (PTableBase<K, V> parent : tables) {
      if (pipeline != parent.getPipeline()) {
        throw new IllegalStateException(
            "Cannot union PTables from different Pipeline instances");
      }
      this.parents.add(parent);
      size += parent.getSize();
    }
  }

  @Override
  protected long getSizeInternal() {
    return size;
  }
  
  @Override
  public PTableType<K, V> getPTableType() {
    return ptype;
  }

  @Override
  public PType<Pair<K, V>> getPType() {
    return ptype;
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.<PCollectionImpl<?>> copyOf(parents);
  }

  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitUnionCollection(new UnionCollection<Pair<K, V>>(
        parents));
  }

  @Override
  public DoNode createDoNode() {
    throw new UnsupportedOperationException(
        "Unioned table does not support do nodes");
  }

}
