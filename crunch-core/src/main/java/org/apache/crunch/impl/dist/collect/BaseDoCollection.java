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
import org.apache.crunch.DoFn;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.DelegatingReadableData;

import java.util.List;

public class BaseDoCollection<S> extends PCollectionImpl<S> {

  private final PCollectionImpl<Object> parent;
  protected final DoFn<Object, S> fn;
  protected final PType<S> ptype;

  protected <T> BaseDoCollection(
      String name,
      PCollectionImpl<T> parent,
      DoFn<T, S> fn,
      PType<S> ptype,
      ParallelDoOptions options) {
    super(name, parent.getPipeline(), options);
    this.parent = (PCollectionImpl<Object>) parent;
    this.fn = (DoFn<Object, S>) fn;
    this.ptype = ptype;
  }

  @Override
  protected long getSizeInternal() {
    return (long) (fn.scaleFactor() * parent.getSize());
  }

  @Override
  protected ReadableData<S> getReadableDataInternal() {
    if (getOnlyParent() instanceof BaseGroupedTable) {
      return materializedData();
    }
    return new DelegatingReadableData(getOnlyParent().asReadable(false), fn);
  }

  @Override
  public PType<S> getPType() {
    return ptype;
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
    visitor.visitDoCollection(this);
  }
}
