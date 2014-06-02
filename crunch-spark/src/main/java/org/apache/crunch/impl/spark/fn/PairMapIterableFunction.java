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
package org.apache.crunch.impl.spark.fn;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class PairMapIterableFunction<K, V, S, T> implements PairFunction<Pair<K, List<V>>, S, Iterable<T>> {

  private final MapFn<Pair<K, List<V>>, Pair<S, Iterable<T>>> fn;
  private final SparkRuntimeContext runtimeContext;
  private boolean initialized;

  public PairMapIterableFunction(
      MapFn<Pair<K, List<V>>, Pair<S, Iterable<T>>> fn,
      SparkRuntimeContext runtimeContext) {
    this.fn = fn;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public Tuple2<S, Iterable<T>> call(Pair<K, List<V>> input) throws Exception {
    if (!initialized) {
      runtimeContext.initialize(fn, null);
      initialized = true;
    }
    Pair<S, Iterable<T>> out = fn.map(input);
    return new Tuple2<S, Iterable<T>>(out.first(), out.second());
  }
}
