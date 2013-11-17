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
package org.apache.crunch.impl.spark.collect;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.impl.dist.collect.BaseDoTable;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.fn.PairFlatMapDoFn;
import org.apache.crunch.impl.spark.fn.PairFlatMapPairDoFn;
import org.apache.crunch.types.PTableType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;

public class DoTable<K, V> extends BaseDoTable<K, V> implements SparkCollection {

  private JavaRDDLike<?, ?> rdd;

  <S> DoTable(String name, PCollectionImpl<S> parent, DoFn<S, Pair<K, V>> fn, PTableType<K, V> ntype,
              ParallelDoOptions options) {
    super(name, parent, fn, ntype, options);
  }

  <S> DoTable(
      String name,
      PCollectionImpl<S> parent,
      CombineFn<K, V> combineFn,
      DoFn<S, Pair<K, V>> fn,
      PTableType<K, V> ntype) {
    super(name, parent, combineFn, fn, ntype, ParallelDoOptions.builder().build());
  }

  public JavaRDDLike<?, ?> getJavaRDDLike(SparkRuntime runtime) {
    if (!runtime.isValid(rdd)) {
      rdd = getJavaRDDLikeInternal(runtime);
      rdd.rdd().setName(getName());
      StorageLevel sl = runtime.getStorageLevel(this);
      if (sl != null) {
        rdd.rdd().persist(sl);
      }
    }
    return rdd;
  }

  private JavaRDDLike<?, ?> getJavaRDDLikeInternal(SparkRuntime runtime) {
    if (combineFn instanceof CombineFn && getOnlyParent() instanceof PGroupedTableImpl) {
      runtime.setCombineFn((CombineFn) combineFn);
    }
    JavaRDDLike<?, ?> parentRDD = ((SparkCollection) getOnlyParent()).getJavaRDDLike(runtime);
    fn.configure(runtime.getConfiguration());
    if (parentRDD instanceof JavaRDD) {
      return ((JavaRDD) parentRDD).mapPartitions(new PairFlatMapDoFn(fn, runtime.getRuntimeContext()));
    } else {
      return ((JavaPairRDD) parentRDD).mapPartitions(new PairFlatMapPairDoFn(fn, runtime.getRuntimeContext()));
    }
  }
}
