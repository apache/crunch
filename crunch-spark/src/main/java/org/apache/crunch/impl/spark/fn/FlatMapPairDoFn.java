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

import com.google.common.collect.Iterators;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.GuavaUtils;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

public class FlatMapPairDoFn<K, V, T> implements FlatMapFunction<Iterator<Tuple2<K, V>>, T> {
  private final DoFn<Pair<K, V>, T> fn;
  private final SparkRuntimeContext ctxt;

  public FlatMapPairDoFn(DoFn<Pair<K, V>, T> fn, SparkRuntimeContext ctxt) {
    this.fn = fn;
    this.ctxt = ctxt;
  }

  @Override
  public Iterator<T> call(Iterator<Tuple2<K, V>> input) throws Exception {
    ctxt.initialize(fn, null);
    return new CrunchIterable<Pair<K, V>, T>(fn,
        Iterators.transform(input, GuavaUtils.<K, V>tuple2PairFunc())).iterator();
  }
}
