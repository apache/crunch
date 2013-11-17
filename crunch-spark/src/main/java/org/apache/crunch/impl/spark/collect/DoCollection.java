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

import org.apache.crunch.DoFn;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.impl.dist.collect.BaseDoCollection;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.fn.FlatMapDoFn;
import org.apache.crunch.impl.spark.fn.FlatMapPairDoFn;
import org.apache.crunch.types.PType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;

public class DoCollection<S> extends BaseDoCollection<S> implements SparkCollection {

  private JavaRDDLike<?, ?> rdd;

  <T> DoCollection(String name, PCollectionImpl<T> parent, DoFn<T, S> fn, PType<S> ntype,
                   ParallelDoOptions options) {
    super(name, parent, fn, ntype, options);
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
    JavaRDDLike<?, ?> parentRDD = ((SparkCollection) getOnlyParent()).getJavaRDDLike(runtime);
    fn.configure(runtime.getConfiguration());
    if (parentRDD instanceof JavaRDD) {
      return ((JavaRDD) parentRDD).mapPartitions(new FlatMapDoFn(fn, runtime.getRuntimeContext()));
    } else {
      return ((JavaPairRDD) parentRDD).mapPartitions(new FlatMapPairDoFn(fn, runtime.getRuntimeContext()));
    }
  }
}
