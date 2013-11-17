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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.GuavaUtils;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

public class PairFlatMapPairDoFn<K, V, K2, V2> extends PairFlatMapFunction<Iterator<Tuple2<K, V>>, K2, V2> {
  private final DoFn<Pair<K, V>, Pair<K2, V2>> fn;
  private final SparkRuntimeContext ctxt;

  public PairFlatMapPairDoFn(DoFn<Pair<K, V>, Pair<K2, V2>> fn, SparkRuntimeContext ctxt) {
    this.fn = fn;
    this.ctxt = ctxt;
  }

  @Override
  public Iterable<Tuple2<K2, V2>> call(Iterator<Tuple2<K, V>> input) throws Exception {
    ctxt.initialize(fn);
    return Iterables.transform(
        new CrunchIterable<Pair<K, V>, Pair<K2, V2>>(
            fn,
            Iterators.transform(input, GuavaUtils.<K, V>tuple2PairFunc())),
        GuavaUtils.<K2, V2>pair2tupleFunc());
  }
}
