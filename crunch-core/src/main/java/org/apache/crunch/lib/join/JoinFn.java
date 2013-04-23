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
package org.apache.crunch.lib.join;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;

/**
 * Represents a {@link org.apache.crunch.DoFn} for performing joins.
 * 
 * @param <K> Type of the keys.
 * @param <U> Type of the first {@link org.apache.crunch.PTable}'s values
 * @param <V> Type of the second {@link org.apache.crunch.PTable}'s values
 */
public abstract class JoinFn<K, U, V> extends
    DoFn<Pair<Pair<K, Integer>, Iterable<Pair<U, V>>>, Pair<K, Pair<U, V>>> {

  protected PType<K> keyType;
  protected PType<U> leftValueType;

  /**
   * Instantiate with the PType of the value of the left side of the join (used for creating deep
   * copies of values).
   * 
   * @param keyType The PType of the value used as the key of the join
   * @param leftValueType The PType of the value type of the left side of the join
   */
  public JoinFn(PType<K> keyType, PType<U> leftValueType) {
    this.keyType = keyType;
    this.leftValueType = leftValueType;
  }

  @Override
  public void initialize() {
    this.keyType.initialize(getConfiguration());
    this.leftValueType.initialize(getConfiguration());
  }

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
