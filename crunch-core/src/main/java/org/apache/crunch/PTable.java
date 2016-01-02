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

import java.util.Collection;
import java.util.Map;

import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

/**
 * A sub-interface of {@code PCollection} that represents an immutable,
 * distributed multi-map of keys and values.
 *
 */
public interface PTable<K, V> extends PCollection<Pair<K, V>> {

  /**
   Returns a {@code PTable} instance that acts as the union of this
   * {@code PTable} and the other {@code PTable}s.
   */
  PTable<K, V> union(PTable<K, V> other);
  
  /**
   * Returns a {@code PTable} instance that acts as the union of this
   * {@code PTable} and the input {@code PTable}s.
   */
  PTable<K, V> union(PTable<K, V>... others);

  /**
   * Performs a grouping operation on the keys of this table.
   *
   * @return a {@code PGroupedTable} instance that represents the grouping
   */
  PGroupedTable<K, V> groupByKey();

  /**
   * Performs a grouping operation on the keys of this table, using the given
   * number of partitions.
   *
   * @param numPartitions
   *          The number of partitions for the data.
   * @return a {@code PGroupedTable} instance that represents this grouping
   */
  PGroupedTable<K, V> groupByKey(int numPartitions);

  /**
   * Performs a grouping operation on the keys of this table, using the
   * additional {@code GroupingOptions} to control how the grouping is executed.
   *
   * @param options
   *          The grouping options to use
   * @return a {@code PGroupedTable} instance that represents the grouping
   */
  PGroupedTable<K, V> groupByKey(GroupingOptions options);

  /**
   * Writes this {@code PTable} to the given {@code Target}.
   */
  PTable<K, V> write(Target target);

  /**
   * Writes this {@code PTable} to the given {@code Target}, using the
   * given {@code Target.WriteMode} to handle existing targets.
   */
  PTable<K, V> write(Target target, Target.WriteMode writeMode);

  PTable<K, V> cache();

  PTable<K, V> cache(CachingOptions options);

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

  /**
   * Returns a {@code PTable} that has the same keys as this instance, but
   * uses the given function to map the values.
   */
  <U> PTable<K, U> mapValues(MapFn<V, U> mapFn, PType<U> ptype);

  /**
   * Returns a {@code PTable} that has the same keys as this instance, but
   * uses the given function to map the values.
   */
  <U> PTable<K, U> mapValues(String name, MapFn<V, U> mapFn, PType<U> ptype);

  /**
   * Returns a {@code PTable} that has the same values as this instance, but
   * uses the given function to map the keys.
   */
  <K2> PTable<K2, V> mapKeys(MapFn<K, K2> mapFn, PType<K2> ptype);

  /**
   * Returns a {@code PTable} that has the same values as this instance, but
   * uses the given function to map the keys.
   */
  <K2> PTable<K2, V> mapKeys(String name, MapFn<K, K2> mapFn, PType<K2> ptype);
  
  /**
   * Aggregate all of the values with the same key into a single key-value pair
   * in the returned PTable.
   */
  PTable<K, Collection<V>> collectValues();

  /**
   * Apply the given filter function to this instance and return the resulting
   * {@code PTable}.
   */
  PTable<K, V> filter(FilterFn<Pair<K, V>> filterFn);

  /**
   * Apply the given filter function to this instance and return the resulting
   * {@code PTable}.
   *
   * @param name
   *          An identifier for this processing step
   * @param filterFn
   *          The {@code FilterFn} to apply
   */
  PTable<K, V> filter(String name, FilterFn<Pair<K, V>> filterFn);

  /**
   * Returns a PTable made up of the pairs in this PTable with the largest value
   * field.
   *
   * @param count
   *          The number of pairs to return
   */
  PTable<K, V> top(int count);

  /**
   * Returns a PTable made up of the pairs in this PTable with the smallest
   * value field.
   *
   * @param count
   *          The number of pairs to return
   */
  PTable<K, V> bottom(int count);

  /**
   * Perform an inner join on this table and the one passed in as an argument on
   * their common keys.
   */
  <U> PTable<K, Pair<V, U>> join(PTable<K, U> other);

  /**
   * Co-group operation with the given table on common keys.
   */
  <U> PTable<K, Pair<Collection<V>, Collection<U>>> cogroup(PTable<K, U> other);

  /**
   * Returns a {@link PCollection} made up of the keys in this PTable.
   */
  PCollection<K> keys();

  /**
   * Returns a {@link PCollection} made up of the values in this PTable.
   */
  PCollection<V> values();

  /**
   * Returns a Map<K, V> made up of the keys and values in this PTable.
   * <p>
   * <b>Note:</b> The contents of the returned map may not be exactly the same
   * as this PTable, as a PTable is a multi-map (i.e. can contain multiple
   * values for a single key).
   */
  Map<K, V> materializeToMap();

  /**
   * Returns a {@link PObject} encapsulating a {@link Map} made up of the keys and values in this
   * {@code PTable}.
   * <p><b>Note:</b>The contents of the returned map may not be exactly the same as this PTable,
   * as a PTable is a multi-map (i.e. can contain multiple values for a single key).
   * </p>
   *
   * @return The {@code PObject} encapsulating a {@code Map} made up of the keys and values in
   * this {@code PTable}.
   */
  PObject<Map<K, V>> asMap();

}
