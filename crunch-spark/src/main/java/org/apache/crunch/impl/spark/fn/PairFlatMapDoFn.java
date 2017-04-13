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
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.GuavaUtils;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

public class PairFlatMapDoFn<T, K, V> implements PairFlatMapFunction<Iterator<T>, K, V> {
  private final DoFn<T, Pair<K, V>> fn;
  private final SparkRuntimeContext ctxt;

  public PairFlatMapDoFn(DoFn<T, Pair<K, V>> fn, SparkRuntimeContext ctxt) {
    this.fn = fn;
    this.ctxt = ctxt;
  }

  @Override
  public Iterator<Tuple2<K, V>> call(Iterator<T> input) throws Exception {
    ctxt.initialize(fn, null);
    return Iterables.transform(
        new CrunchIterable<T, Pair<K, V>>(fn, input),
        GuavaUtils.<K, V>pair2tupleFunc()).iterator();
  }
}
