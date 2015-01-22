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

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import java.util.List;

public interface PCollectionFactory {

  <S> BaseInputCollection<S> createInputCollection(
      Source<S> source,
      String named,
      DistributedPipeline distributedPipeline,
      ParallelDoOptions doOpts);

  <K, V> BaseInputTable<K, V> createInputTable(
      TableSource<K,V> source,
      String named,
      DistributedPipeline distributedPipeline,
      ParallelDoOptions doOpts);

  <S> BaseUnionCollection<S> createUnionCollection(List<? extends PCollectionImpl<S>> internal);

  <S, T> BaseDoCollection<T> createDoCollection(
      String name,
      PCollectionImpl<S> chainingCollection,
      DoFn<S,T> fn,
      PType<T> type,
      ParallelDoOptions options);

  <S, K, V> BaseDoTable<K, V> createDoTable(
      String name,
      PCollectionImpl<S> chainingCollection,
      DoFn<S,Pair<K, V>> fn,
      PTableType<K, V> type,
      ParallelDoOptions options);

  <S, K, V> BaseDoTable<K, V> createDoTable(
      String name,
      PCollectionImpl<S> chainingCollection,
      CombineFn<K, V> combineFn,
      DoFn<S,Pair<K, V>> fn,
      PTableType<K, V> type);

  <K, V> BaseGroupedTable<K, V> createGroupedTable(PTableBase<K,V> parent, GroupingOptions groupingOptions);

  <K, V> PTable<K, V> createUnionTable(List<PTableBase<K, V>> internal);
}
