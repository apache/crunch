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

import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.dist.collect.BaseUnionTable;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.dist.collect.PTableBase;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.fn.PairFlatMapDoFn;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

public class UnionTable<K, V> extends BaseUnionTable<K, V> implements SparkCollection {

  private JavaRDDLike<?, ?> rdd;

  UnionTable(List<PTableBase<K, V>> tables) {
    super(tables);
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
    List<PCollectionImpl<?>> parents = getParents();
    JavaPairRDD[] rdds = new JavaPairRDD[parents.size()];
    for (int i = 0; i < rdds.length; i++) {
      if (parents.get(i) instanceof PTableBase) {
        rdds[i] = (JavaPairRDD) ((SparkCollection) parents.get(i)).getJavaRDDLike(runtime);
      } else {
        JavaRDD rdd = (JavaRDD) ((SparkCollection) parents.get(i)).getJavaRDDLike(runtime);
        rdds[i] = rdd.mapPartitionsToPair(new PairFlatMapDoFn(IdentityFn.getInstance(), runtime.getRuntimeContext()));
      }
    }
    return runtime.getSparkContext().union(rdds);
  }
}
