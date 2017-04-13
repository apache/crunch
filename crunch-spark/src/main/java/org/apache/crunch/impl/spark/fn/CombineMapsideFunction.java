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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CombineMapsideFunction<K, V> implements PairFlatMapFunction<Iterator<Tuple2<K, V>>, K, V> {

  private static final int REDUCE_EVERY_N = 50000;

  private final CombineFn<K,V> combineFn;
  private final SparkRuntimeContext ctxt;

  public CombineMapsideFunction(CombineFn<K, V> combineFn, SparkRuntimeContext ctxt) {
    this.combineFn = combineFn;
    this.ctxt = ctxt;
  }

  @Override
  public Iterator<Tuple2<K, V>> call(Iterator<Tuple2<K, V>> iter) throws Exception {
    ctxt.initialize(combineFn, null);
    Map<K, List<V>> cache = Maps.newHashMap();
    int cnt = 0;
    while (iter.hasNext()) {
      Tuple2<K, V> t = iter.next();
      List<V> values = cache.get(t._1());
      if (values == null) {
        values = Lists.newArrayList();
        cache.put(t._1(), values);
      }
      values.add(t._2());
      cnt++;
      if (cnt % REDUCE_EVERY_N == 0) {
        cache = reduce(cache);
      }
    }

    return new Flattener<K, V>(cache).iterator();
  }

  private Map<K, List<V>> reduce(Map<K, List<V>> cache) {
    Set<K> keys = cache.keySet();
    Map<K, List<V>> res = Maps.newHashMap();
    for (K key : keys) {
      for (Pair<K, V> p : reduce(key, cache.get(key))) {
        List<V> values = res.get(p.first());
        if (values == null) {
          values = Lists.newArrayList();
          res.put(p.first(), values);
        }
        values.add(p.second());
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

  private static class Flattener<K, V> implements Iterable<Tuple2<K, V>> {
    private final Map<K, List<V>> entries;

    public Flattener(Map<K, List<V>> entries) {
      this.entries = entries;
    }

    @Override
    public Iterator<Tuple2<K, V>> iterator() {
      return new UnmodifiableIterator<Tuple2<K, V>>() {
        private Iterator<K> keyIter = entries.keySet().iterator();
        private K currentKey;
        private Iterator<V> valueIter = null;

        @Override
        public boolean hasNext() {
          while (valueIter == null || !valueIter.hasNext()) {
            if (keyIter.hasNext()) {
              currentKey = keyIter.next();
              valueIter = entries.get(currentKey).iterator();
            } else {
              return false;
            }
          }
          return true;
        }

        @Override
        public Tuple2<K, V> next() {
          return new Tuple2<K, V>(currentKey, valueIter.next());
        }
      };
    }
  }
}
