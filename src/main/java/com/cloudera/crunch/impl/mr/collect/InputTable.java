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
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;
import com.google.common.collect.ImmutableList;

public class InputTable<K, V> extends PTableBase<K, V> {

  private final TableSource<K, V> source;
  private final InputCollection<Pair<K, V>> asCollection;
  
  public InputTable(TableSource<K, V> source, MRPipeline pipeline) {
    super(source.toString());
    this.source = source;
    this.pipeline = pipeline;
    this.asCollection = new InputCollection<Pair<K, V>>(source, pipeline);
  }

  @Override
  public long getSize() {
    return asCollection.getSize();
  }

  @Override
  public PTableType<K, V> getPTableType() {
    return source.getTableType();
  }

  @Override
  public PType<Pair<K, V>> getPType() {
    return source.getType();
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.of();
  }

  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitInputCollection(asCollection);
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createInputNode(source);
  }
}
