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

import org.apache.crunch.Pair;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

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

  public TableSource<K, V> getSource() {
    return source;
  }
  
  @Override
  protected long getSizeInternal() {
    return asCollection.getSizeInternal();
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

  @Override
  public int hashCode() {
    return asCollection.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return asCollection.equals(other);
  }
}
