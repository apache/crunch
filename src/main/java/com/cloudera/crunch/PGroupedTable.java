/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.crunch;

/**
 * The Crunch representation of a grouped {@link PTable}.
 *
 */
public interface PGroupedTable<K, V> extends PCollection<Pair<K, Iterable<V>>> {
  /**
   * Combines the values of this grouping using the given {@code CombineFn}.
   * 
   * @param combineFn The combiner function
   * @return A {@code PTable} where each key has a single value
   */
  PTable<K, V> combineValues(CombineFn<K, V> combineFn);

  /**
   * Convert this grouping back into a multimap.
   * 
   * @return an ungrouped version of the data in this {@code PGroupedTable}.
   */
  PTable<K, V> ungroup();
}
