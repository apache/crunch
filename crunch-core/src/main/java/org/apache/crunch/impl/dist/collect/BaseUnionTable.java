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
import com.google.common.collect.Lists;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.UnionReadableData;

import java.util.List;

public class BaseUnionTable<K, V> extends PTableBase<K, V> {

  private PTableType<K, V> ptype;
  private List<PCollectionImpl<Pair<K, V>>> parents;
  private long size;
  private long lastModifiedAt = -1;
  
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

  protected BaseUnionTable(List<PTableBase<K, V>> tables) {
    super(flatName(tables), tables.get(0).getPipeline());
    this.ptype = tables.get(0).getPTableType();
    this.pipeline = tables.get(0).getPipeline();
    this.parents = Lists.newArrayList();
    for (PTableBase<K, V> parent : tables) {
      if (pipeline != parent.getPipeline()) {
        throw new IllegalStateException("Cannot union PTables from different Pipeline instances");
      }
      this.parents.add(parent);
      size += parent.getSize();
    }
  }

  @Override
  public void setBreakpoint() {
    super.setBreakpoint();
    for (PCollectionImpl<?> parent : getParents()) {
      parent.setBreakpoint();
    }
  }

  @Override
  protected long getSizeInternal() {
    return size;
  }

  @Override
  public long getLastModifiedAt() {
    if (lastModifiedAt == -1) {
      for (PCollectionImpl<Pair<K, V>> parent : parents) {
        long parentLastModifiedAt = parent.getLastModifiedAt();
        if (parentLastModifiedAt > lastModifiedAt) {
          lastModifiedAt = parentLastModifiedAt;
        }
      }
    }
    return lastModifiedAt;
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
    return ImmutableList.<PCollectionImpl<?>>copyOf(parents);
  }

  @Override
  protected void acceptInternal(Visitor visitor) {
    visitor.visitUnionCollection(pipeline.getFactory().createUnionCollection(parents));
  }

  @Override
  protected ReadableData<Pair<K, V>> getReadableDataInternal() {
    List<ReadableData<Pair<K, V>>> prds = Lists.newArrayList();
    for (PCollectionImpl<Pair<K, V>> parent : parents) {
      if (parent instanceof BaseGroupedTable) {
        return materializedData();
      } else {
        prds.add(parent.asReadable(false));
      }
    }
    return new UnionReadableData<Pair<K, V>>(prds);
  }
}
