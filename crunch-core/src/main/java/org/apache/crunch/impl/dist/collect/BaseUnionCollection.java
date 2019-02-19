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
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.UnionReadableData;

import java.util.List;

public class BaseUnionCollection<S> extends PCollectionImpl<S> {

  private List<PCollectionImpl<S>> parents;
  private long size = 0;
  private long lastModifiedAt = -1;
  
  private static String flatName(List<? extends PCollectionImpl> collections) {
    StringBuilder sb = new StringBuilder("union(");
    for (int i = 0; i < collections.size(); i++) {
      if (i != 0) {
        sb.append(',');
      }
      sb.append(collections.get(i).getName());
    }
    return sb.append(')').toString();
  }

  protected BaseUnionCollection(List<? extends PCollectionImpl<S>> collections) {
    super(flatName(collections), collections.get(0).getPipeline());
    this.parents = ImmutableList.copyOf(collections);
    for (PCollectionImpl<S> parent : parents) {
      if (this.pipeline != parent.getPipeline()) {
        throw new IllegalStateException("Cannot union PCollections from different Pipeline instances");
      }
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
      for (PCollectionImpl<S> parent : parents) {
        long parentLastModifiedAt = parent.getLastModifiedAt();
        if (parentLastModifiedAt > lastModifiedAt) {
          lastModifiedAt = parentLastModifiedAt;
        }
      }
    }
    return lastModifiedAt;
  }
  
  @Override
  protected ReadableData<S> getReadableDataInternal() {
    List<ReadableData<S>> prds = Lists.newArrayList();
    for (PCollectionImpl<S> parent : parents) {
      if (parent instanceof BaseGroupedTable) {
        return materializedData();
      } else {
        prds.add(parent.asReadable(false));
      }
    }
    return new UnionReadableData<S>(prds);
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
  protected void acceptInternal(Visitor visitor) {
    visitor.visitUnionCollection(this);
  }
}
