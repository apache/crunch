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

import com.google.common.collect.ImmutableList;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.types.PTableType;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

public class EmptyPTable<K, V> extends org.apache.crunch.impl.dist.collect.EmptyPTable<K, V> implements SparkCollection {

  public EmptyPTable(DistributedPipeline pipeline, PTableType<K, V> ptype) {
    super(pipeline, ptype);
  }

  @Override
  public JavaRDDLike<?, ?> getJavaRDDLike(SparkRuntime runtime) {
    return runtime.getSparkContext().parallelizePairs(ImmutableList.<Tuple2<K, V>>of());
  }
}
