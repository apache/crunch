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
package org.apache.crunch;

import org.apache.crunch.Aggregator;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PType;

/**
 * The Crunch representation of a grouped {@link PTable}, which corresponds to the output of
 * the shuffle phase of a MapReduce job.
 */
public interface PGroupedTable<K, V> extends PCollection<Pair<K, Iterable<V>>> {

  /**
   * Combines the values of this grouping using the given {@code CombineFn}.
   * 
   * @param combineFn
   *          The combiner function
   * @return A {@code PTable} where each key has a single value
   */
  PTable<K, V> combineValues(CombineFn<K, V> combineFn);
  
  /**
   * Combines and reduces the values of this grouping using the given {@code CombineFn} instances.
   * 
   * @param combineFn
   *          The combiner function during the combine phase
   * @param reduceFn
   *          The combiner function during the reduce phase
   * @return A {@code PTable} where each key has a single value
   */
  PTable<K, V> combineValues(CombineFn<K, V> combineFn, CombineFn<K, V> reduceFn);

  /**
   * Combine the values in each group using the given {@link Aggregator}.
   *
   * @param aggregator The function to use
   * @return A {@link PTable} where each group key maps to an aggregated
   *         value. Group keys may be repeated if an aggregator returns
   *         more than one value.
   */
  PTable<K, V> combineValues(Aggregator<V> aggregator);

  /**
   * Combine and reduces the values in each group using the given {@link Aggregator} instances.
   *
   * @param combineAggregator The aggregator to use during the combine phase
   * @param reduceAggregator The aggregator to use during the reduce phase
   * @return A {@link PTable} where each group key maps to an aggregated
   *         value. Group keys may be repeated if an aggregator returns
   *         more than one value.
   */
  PTable<K, V> combineValues(Aggregator<V> combineAggregator, Aggregator<V> reduceAggregator);

  /**
   * Maps the {@code Iterable<V>} elements of each record to a new type. Just like
   * any {@code parallelDo} operation on a {@code PGroupedTable}, this may only be
   * called once.
   * 
   * @param mapFn The mapping function
   * @param ptype The serialization information for the returned data
   * @return A new {@code PTable} instance
   */
  <U> PTable<K, U> mapValues(MapFn<Iterable<V>, U> mapFn, PType<U> ptype);


  /**
   * Maps the {@code Iterable<V>} elements of each record to a new type. Just like
   * any {@code parallelDo} operation on a {@code PGroupedTable}, this may only be
   * called once. Designed for Java lambdas
   * @param mapFn The mapping function (can be lambda/method ref)
   * @param ptype The serialization infromation for the returned data
   * @return A new {@code PTable} instance
   */
  <U> PTable<K, U> mapValues(IMapFn<Iterable<V>, U> mapFn, PType<U> ptype);
  
  /**
   * Maps the {@code Iterable<V>} elements of each record to a new type. Just like
   * any {@code parallelDo} operation on a {@code PGroupedTable}, this may only be
   * called once.
   * 
   * @param name A name for this operation
   * @param mapFn The mapping function
   * @param ptype The serialization information for the returned data
   * @return A new {@code PTable} instance
   */
  <U> PTable<K, U> mapValues(String name, MapFn<Iterable<V>, U> mapFn, PType<U> ptype);

  /**
   * Convert this grouping back into a multimap.
   * 
   * @return an ungrouped version of the data in this {@code PGroupedTable}.
   */
  PTable<K, V> ungroup();
  
  /**
   * Return the {@code PGroupedTableType} containing serialization information for
   * this {@code PGroupedTable}.
   */
  PGroupedTableType<K, V> getGroupedTableType();
}
