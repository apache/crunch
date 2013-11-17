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

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CombineMapsideFunction<K, V> extends PairFlatMapFunction<Iterator<Tuple2<K, V>>, K, V> {

  private static final int REDUCE_EVERY_N = 50000;

  private final CombineFn<K,V> combineFn;
  private final SparkRuntimeContext ctxt;

  public CombineMapsideFunction(CombineFn<K, V> combineFn, SparkRuntimeContext ctxt) {
    this.combineFn = combineFn;
    this.ctxt = ctxt;
  }

  @Override
  public Iterable<Tuple2<K, V>> call(Iterator<Tuple2<K, V>> iter) throws Exception {
    ctxt.initialize(combineFn);
    Multimap<K, V> cache = HashMultimap.create();
    int cnt = 0;
    while (iter.hasNext()) {
      Tuple2<K, V> t = iter.next();
      cache.put(t._1, t._2);
      cnt++;
      if (cnt % REDUCE_EVERY_N == 0) {
        cache = reduce(cache);
      }
    }

    return Iterables.transform(reduce(cache).entries(), new Function<Map.Entry<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> apply(Map.Entry<K, V> input) {
        return new Tuple2<K, V>(input.getKey(), input.getValue());
      }
    });
  }

  private Multimap<K, V> reduce(Multimap<K, V> cache) {
    Set<K> keys = cache.keySet();
    Multimap<K, V> res = HashMultimap.create(keys.size(), keys.size());
    for (K key : keys) {
      for (Pair<K, V> p : reduce(key, cache.get(key))) {
        res.put(p.first(), p.second());
      }
    }
    return res;
  }

  private List<Pair<K, V>> reduce(K key, Iterable<V> values) {
    InMemoryEmitter<Pair<K, V>> emitter = new InMemoryEmitter<Pair<K, V>>();
    combineFn.process(Pair.of(key, values), emitter);
    combineFn.cleanup(emitter);
    return emitter.getOutput();
  }
}
