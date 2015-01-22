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

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.BaseDoCollection;
import org.apache.crunch.impl.dist.collect.BaseDoTable;
import org.apache.crunch.impl.dist.collect.BaseGroupedTable;
import org.apache.crunch.impl.dist.collect.BaseInputCollection;
import org.apache.crunch.impl.dist.collect.BaseInputTable;
import org.apache.crunch.impl.dist.collect.BaseUnionCollection;
import org.apache.crunch.impl.dist.collect.PCollectionFactory;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.dist.collect.PTableBase;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.collect.DoCollection;
import org.apache.crunch.impl.mr.collect.DoTable;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.InputTable;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.collect.UnionCollection;
import org.apache.crunch.impl.mr.collect.UnionTable;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import java.util.List;

public class MRCollectionFactory implements PCollectionFactory {
  @Override
  public <S> BaseInputCollection<S> createInputCollection(
      Source<S> source,
      String name,
      DistributedPipeline pipeline,
      ParallelDoOptions doOpts) {
    return new InputCollection<S>(source, name, (MRPipeline) pipeline, doOpts);
  }

  @Override
  public <K, V> BaseInputTable<K, V> createInputTable(
      TableSource<K, V> source,
      String name,
      DistributedPipeline pipeline,
      ParallelDoOptions doOpts) {
    return new InputTable<K, V>(source, name, (MRPipeline) pipeline, doOpts);
  }

  @Override
  public <S> BaseUnionCollection<S> createUnionCollection(List<? extends PCollectionImpl<S>> internal) {
    return new UnionCollection<S>(internal);
  }

  @Override
  public <S, T> BaseDoCollection<T> createDoCollection(
        String name,
        PCollectionImpl<S> parent,
        DoFn<S, T> fn,
        PType<T> type,
        ParallelDoOptions options) {
    return new DoCollection<T>(name, parent, fn, type, options);
  }

  @Override
  public <S, K, V> BaseDoTable<K, V> createDoTable(
      String name,
      PCollectionImpl<S> parent,
      DoFn<S, Pair<K, V>> fn,
      PTableType<K, V> type,
      ParallelDoOptions options) {
    return new DoTable<K, V>(name, parent, fn, type, options);
  }

  @Override
  public <S, K, V> BaseDoTable<K, V> createDoTable(
      String name,
      PCollectionImpl<S> parent,
      CombineFn<K, V> combineFn,
      DoFn<S, Pair<K, V>> reduceFn,
      PTableType<K, V> type) {
    return new DoTable<K, V>(name, parent, combineFn, reduceFn, type);
  }

  @Override
  public <K, V> BaseGroupedTable<K, V> createGroupedTable(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    return new PGroupedTableImpl<K, V>(parent, groupingOptions);
  }

  @Override
  public <K, V> PTable<K, V> createUnionTable(List<PTableBase<K, V>> internal) {
    return new UnionTable<K, V>(internal);
  }
}
