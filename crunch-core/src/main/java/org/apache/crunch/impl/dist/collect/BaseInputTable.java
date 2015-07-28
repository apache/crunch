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
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import java.util.List;

public class BaseInputTable<K, V> extends PTableBase<K, V> {

  protected final TableSource<K, V> source;
  protected final BaseInputCollection<Pair<K, V>> asCollection;

  public BaseInputTable(TableSource<K, V> source, DistributedPipeline pipeline) {
    super(source.toString(), pipeline);
    this.source = source;
    this.asCollection = pipeline.getFactory().createInputCollection(
        source, source.toString(), pipeline, ParallelDoOptions.builder().build());
  }

  public BaseInputTable(TableSource<K, V> source, String name, DistributedPipeline pipeline, ParallelDoOptions doOpts) {
    super(source.toString(), pipeline, doOpts);
    this.source = source;
    this.asCollection = pipeline.getFactory().createInputCollection(source, name, pipeline, doOpts);
  }

  public TableSource<K, V> getSource() {
    return source;
  }

  @Override
  protected boolean waitingOnTargets() {
    return asCollection.waitingOnTargets();
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
  protected ReadableData<Pair<K, V>> getReadableDataInternal() {
    return asCollection.getReadableDataInternal();
  }

  @Override
  public long getLastModifiedAt() {
    return source.getLastModifiedAt(pipeline.getConfiguration());
  }

  @Override
  protected void acceptInternal(Visitor visitor) {
    visitor.visitInputCollection(asCollection);
  }

  @Override
  public int hashCode() {
    return asCollection.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof BaseInputTable)) {
      return false;
    }
    return asCollection.equals(other);
  }
}
