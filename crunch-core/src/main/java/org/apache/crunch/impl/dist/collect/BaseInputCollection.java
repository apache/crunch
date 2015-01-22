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
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.types.PType;

import java.util.List;

public class BaseInputCollection<S> extends PCollectionImpl<S> {

  protected final Source<S> source;

  public BaseInputCollection(Source<S> source, DistributedPipeline pipeline) {
    super(source.toString(), pipeline);
    this.source = source;
  }

  public BaseInputCollection(Source<S> source, String name, DistributedPipeline pipeline, ParallelDoOptions doOpts) {
    super(name == null ? source.toString() : name, pipeline, doOpts);
    this.source = source;
  }

  @Override
  protected ReadableData<S> getReadableDataInternal() {
    if (source instanceof ReadableSource) {
      return ((ReadableSource<S>) source).asReadable();
    } else {
      return materializedData();
    }
  }

  @Override
  protected void acceptInternal(Visitor visitor) {
    visitor.visitInputCollection(this);
  }

  @Override
  public PType<S> getPType() {
    return source.getType();
  }

  public Source<S> getSource() {
    return source;
  }

  @Override
  protected boolean waitingOnTargets() {
    return doOptions.getTargets().contains(source);
  }

  @Override
  protected long getSizeInternal() {
    long sz = source.getSize(pipeline.getConfiguration());
    if (sz < 0) {
      throw new IllegalStateException("Input source " + source + " does not exist!");
    }
    return sz;
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.of();
  }

  @Override
  public long getLastModifiedAt() {
    return source.getLastModifiedAt(pipeline.getConfiguration());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof BaseInputCollection)) {
      return false;
    }
    return source.equals(((BaseInputCollection) obj).source);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(source).toHashCode();
  }
}
