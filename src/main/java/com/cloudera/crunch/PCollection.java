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

import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;

/**
 * A representation of an immutable, distributed collection of elements
 * that is the fundamental target of computations in Crunch.
 *
 */
public interface PCollection<S> {
  /**
   * Returns the {@code Pipeline} associated with this PCollection.
   */
  Pipeline getPipeline();
  
  /**
   * Returns a {@code PCollection} instance that acts as the union
   * of this {@code PCollection} and the input {@code PCollection}s.
   */
  PCollection<S> union(PCollection<S>... collections);

  /**
   * Applies the given doFn to the elements of this {@code PCollection} and
   * returns a new {@code PCollection} that is the output of this processing.
   * 
   * @param doFn The {@code DoFn} to apply
   * @param type The {@link PType} of the resulting {@code PCollection}
   * @return a new {@code PCollection}
   */
  <T> PCollection<T> parallelDo(DoFn<S, T> doFn, PType<T> type);
  
  /**
   * Applies the given doFn to the elements of this {@code PCollection} and
   * returns a new {@code PCollection} that is the output of this processing.
   * 
   * @param name An identifier for this processing step, useful for debugging
   * @param doFn The {@code DoFn} to apply
   * @param type The {@link PType} of the resulting {@code PCollection}
   * @return a new {@code PCollection}
   */
  <T> PCollection<T> parallelDo(String name, DoFn<S, T> doFn, PType<T> type);

  /**
   * Similar to the other {@code parallelDo} instance, but returns a
   * {@code PTable} instance instead of a {@code PCollection}.
   * 
   * @param doFn The {@code DoFn} to apply
   * @param type The {@link PTableType} of the resulting {@code PTable}
   * @return a new {@code PTable}
   */
  <K, V> PTable<K, V> parallelDo(DoFn<S, Pair<K, V>> doFn, PTableType<K, V> type);
  
  /**
   * Similar to the other {@code parallelDo} instance, but returns a
   * {@code PTable} instance instead of a {@code PCollection}.
   * 
   * @param name An identifier for this processing step
   * @param doFn The {@code DoFn} to apply
   * @param type The {@link PTableType} of the resulting {@code PTable}
   * @return a new {@code PTable}
   */
  <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> doFn,
      PTableType<K, V> type);

  /**
   * Write the contents of this {@code PCollection} to the given {@code Target},
   * using the storage format specified by the target.
   * 
   * @param target The target to write to
   */
  PCollection<S> write(Target target);
  
  /**
   * Returns a reference to the data set represented by this PCollection that
   * may be used by the client to read the data locally.
   */
  Iterable<S> materialize();
  
  /**
   * Returns the {@code PType} of this {@code PCollection}.
   */
  PType<S> getPType();

  /**
   * Returns the {@code PTypeFamily} of this {@code PCollection}.
   */
  PTypeFamily getTypeFamily();

  /**
   * Returns the size of the data represented by this {@code PCollection} in bytes.
   */
  long getSize();

  /**
   * Returns a shorthand name for this PCollection.
   */
  String getName();
  
  /**
   * Apply the given filter function to this instance and return the
   * resulting {@code PCollection}.
   */
  PCollection<S> filter(FilterFn<S> filterFn);
  
  /**
   * Apply the given filter function to this instance and return the
   * resulting {@code PCollection}.
   * 
   * @param name An identifier for this processing step
   * @param filterFn The {@code FilterFn} to apply
   */
  PCollection<S> filter(String name, FilterFn<S> filterFn);
  
  /**
   * Apply the given map function to each element of this instance in order
   * to create a {@code PTable}.
   */
  <K> PTable<K, S> by(MapFn<S, K> extractKeyFn, PType<K> keyType);
 
  /**
   * Apply the given map function to each element of this instance in order
   * to create a {@code PTable}.
   *   
   * @param name An identifier for this processing step
   * @param extractKeyFn The {@code MapFn} to apply
   */
  <K> PTable<K, S> by(String name, MapFn<S, K> extractKeyFn, PType<K> keyType);
  
  /**
   * Returns a {@code PCollection} instance that contains all of the elements
   * of this instance in sorted order.
   */
  PCollection<S> sort(boolean ascending);
  
  /**
   * Returns a {@code PTable} instance that contains the counts of each unique
   * element of this PCollection.
   */
  PTable<S, Long> count();
  
  /**
   * Returns a {@code PCollection} made up of only the maximum element of this
   * instance.
   */
  PCollection<S> max();
  
  /**
   * Returns a {@code PCollection} made up of only the minimum element of this
   * instance.
   */
  PCollection<S> min();
  
  /**
   * Randomly sample items from this PCollection instance with the given
   * probability of an item being accepted.
   */
  PCollection<S> sample(double acceptanceProbability);
  
  /**
   * Randomly sample items from this PCollection instance with the given
   * probability of an item being accepted and using the given seed.
   */
  PCollection<S> sample(double acceptanceProbability, long seed);
}
