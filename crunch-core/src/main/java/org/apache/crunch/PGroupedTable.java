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

/**
 * The Crunch representation of a grouped {@link PTable}.
 * 
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
   * Combine the values in each group using the given {@link Aggregator}.
   *
   * @param aggregator The function to use
   * @return A {@link PTable} where each group key maps to an aggregated
   *         value. Group keys may be repeated if an aggregator returns
   *         more than one value.
   */
  PTable<K, V> combineValues(Aggregator<V> aggregator);

  /**
   * Convert this grouping back into a multimap.
   * 
   * @return an ungrouped version of the data in this {@code PGroupedTable}.
   */
  PTable<K, V> ungroup();
}
