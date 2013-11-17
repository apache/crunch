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

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Default join strategy that simply sends all data through the map, shuffle, and reduce phase.
 * <p>
 * This join strategy is full-featured (i.e. all methods are available), but is not highly
 * efficient due to its passing all data through the shuffle phase.
 */
public class DefaultJoinStrategy<K, U, V> implements JoinStrategy<K, U, V> {

  private final int numReducers;

  public DefaultJoinStrategy() {
    this(-1);
  }

  public DefaultJoinStrategy(int numReducers) {
    this.numReducers = numReducers;
  }

  @Override
  public PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinType joinType) {
    switch (joinType) {
    case INNER_JOIN:
      return join(left, right, new InnerJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
    case LEFT_OUTER_JOIN:
      return join(left, right, new LeftOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
    case RIGHT_OUTER_JOIN:
      return join(left, right,
        new RightOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
    case FULL_OUTER_JOIN:
      return join(left, right, new FullOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
    default:
      throw new UnsupportedOperationException("Join type " + joinType + " is not supported");
    }
  }

  /**
   * Perform a default join on the given {@code PTable} instances using a user-specified {@code JoinFn}.
   *
   * @param left left table to be joined
   * @param right right table to be joined
   * @param joinFn The user-specified implementation of the {@code JoinFn} class
   * @return joined tables
   */
  public PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinFn<K, U, V> joinFn) {
    PTypeFamily ptf = left.getTypeFamily();
    PGroupedTable<Pair<K, Integer>, Pair<U, V>> grouped = preJoin(left, right, numReducers);
    PTableType<K, Pair<U, V>> ret = ptf
        .tableOf(left.getKeyType(), ptf.pairs(left.getValueType(), right.getValueType()));

    return grouped.parallelDo(joinFn.getJoinType() + grouped.getName(), joinFn, ret);
  }

  static <K, U, V> PGroupedTable<Pair<K, Integer>, Pair<U, V>> preJoin(PTable<K, U> left, PTable<K, V> right,
                                                                       int numReducers) {
    PTypeFamily ptf = left.getTypeFamily();
    PTableType<Pair<K, Integer>, Pair<U, V>> ptt = ptf.tableOf(ptf.pairs(left.getKeyType(), ptf.ints()),
        ptf.pairs(left.getValueType(), right.getValueType()));

    PTable<Pair<K, Integer>, Pair<U, V>> tag1 = left.parallelDo("joinTagLeft",
        new MapFn<Pair<K, U>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, U> input) {
            return Pair.of(Pair.of(input.first(), 0), Pair.of(input.second(), (V) null));
          }
        }, ptt);
    PTable<Pair<K, Integer>, Pair<U, V>> tag2 = right.parallelDo("joinTagRight",
        new MapFn<Pair<K, V>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, V> input) {
            return Pair.of(Pair.of(input.first(), 1), Pair.of((U) null, input.second()));
          }
        }, ptt);

    GroupingOptions.Builder optionsBuilder = GroupingOptions.builder();
    optionsBuilder.requireSortedKeys();
    optionsBuilder.partitionerClass(JoinUtils.getPartitionerClass(ptf));
    if (numReducers > 0) {
      optionsBuilder.numReducers(numReducers);
    }
    return (tag1.union(tag2)).groupByKey(optionsBuilder.build());
  }



}
