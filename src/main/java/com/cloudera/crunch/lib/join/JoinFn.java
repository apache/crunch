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
package com.cloudera.crunch.lib.join;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;

/**
 * Represents a {@link com.cloudera.crunch.DoFn} for performing joins.
 *
 * @param <K> Type of the keys.
 * @param <U> Type of the first {@link com.cloudera.crunch.PTable}'s values
 * @param <V> Type of the second {@link com.cloudera.crunch.PTable}'s values
 */
public abstract class JoinFn<K, U, V>
    extends DoFn<Pair<Pair<K, Integer>, Iterable<Pair<U, V>>>, Pair<K, Pair<U, V>>> {
  /** @return The name of this join type (e.g. innerJoin, leftOuterJoin). */
  public abstract String getJoinType();

  /**
   * Performs the actual joining.
   *
   * @param key The key for this grouping of values.
   * @param id The side that this group of values is from (0 -> left, 1 -> right).
   * @param pairs The group of values associated with this key and id pair.
   * @param emitter The emitter to send the output to.
   */
  public abstract void join(K key, int id, Iterable<Pair<U, V>> pairs,
      Emitter<Pair<K, Pair<U, V>>> emitter);

  /**
   * Split up the input record to make coding a bit more manageable.
   *
   * @param input The input record.
   * @param emitter The emitter to send the output to.
   */
  @Override
  public void process(Pair<Pair<K, Integer>, Iterable<Pair<U, V>>> input,
      Emitter<Pair<K, Pair<U, V>>> emitter) {
    join(input.first().first(), input.first().second(), input.second(), emitter);
  }
}
