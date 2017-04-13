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
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.GuavaUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

public class CrunchPairTuple2<K, V> implements PairFlatMapFunction<Iterator<Pair<K, V>>, K, V> {
  @Override
  public Iterator<Tuple2<K, V>> call(final Iterator<Pair<K, V>> iterator) throws Exception {
    return Iterators.transform(iterator, GuavaUtils.<K, V>pair2tupleFunc());
  }
}
