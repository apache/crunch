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
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Tuple2MapFunction<K, V> implements PairFunction<Pair<K, V>, K, V> {
  private final MapFn<Pair<K, V>, Pair<K, V>> fn;
  private final SparkRuntimeContext ctxt;
  private boolean initialized;

  public Tuple2MapFunction(MapFn<Pair<K, V>, Pair<K, V>> fn, SparkRuntimeContext ctxt) {
    this.fn = fn;
    this.ctxt = ctxt;
  }

  @Override
  public Tuple2<K, V> call(Pair<K, V> p) throws Exception {
    if (!initialized) {
      ctxt.initialize(fn, null);
      initialized = true;
    }
    Pair<K, V> res = fn.map(p);
    return new Tuple2<K, V>(res.first(), res.second());
  }
}
