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

import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;

/**
 * A sub-interface of {@code PCollection} that represents an immutable,
 * distributed multi-map of keys and values.
 *
 */
public interface PTable<K, V> extends PCollection<Pair<K, V>> {
  /**
   * Returns a {@code PTable} instance that acts as the union
   * of this {@code PTable} and the input {@code PTable}s.
   */
  PTable<K, V> union(PTable<K, V>... others);

  /**
   * Performs a grouping operation on the keys of this table.
   * @return a {@code PGroupedTable} instance that represents the grouping
   */
  PGroupedTable<K, V> groupByKey();

  /**
   * Performs a grouping operation on the keys of this table, using the given
   * number of partitions.
   * 
   * @param numPartitions The number of partitions for the data.
   * @return a {@code PGroupedTable} instance that represents this grouping
   */
  PGroupedTable<K, V> groupByKey(int numPartitions);
  
  /**
   * Performs a grouping operation on the keys of this table, using the
   * additional {@code GroupingOptions} to control how the grouping is
   * executed.
   * 
   * @param options The grouping options to use
   * @return a {@code PGroupedTable} instance that represents the grouping
   */
  PGroupedTable<K, V> groupByKey(GroupingOptions options);

  PTable<K, V> write(Target target);
  
  /**
   * Returns the {@code PTableType} of this {@code PTable}.
   */
  PTableType<K, V> getPTableType();
  
  /**
   * Returns the {@code PType} of the key.
   */
  PType<K> getKeyType();

  /**
   * Returns the {@code PType} of the value.
   */
  PType<V> getValueType();
}
