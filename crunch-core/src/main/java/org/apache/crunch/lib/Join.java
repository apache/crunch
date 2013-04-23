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
package org.apache.crunch.lib;

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.FullOuterJoinFn;
import org.apache.crunch.lib.join.InnerJoinFn;
import org.apache.crunch.lib.join.JoinFn;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.lib.join.LeftOuterJoinFn;
import org.apache.crunch.lib.join.RightOuterJoinFn;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Utilities for joining multiple {@code PTable} instances based on a common
 * lastKey.
 */
public class Join {
  /**
   * Performs an inner join on the specified {@link PTable}s.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join">Inner
   *      Join</a>
   * @param left
   *          A PTable to perform an inner join on.
   * @param right
   *          A PTable to perform an inner join on.
   * @param <K>
   *          Type of the keys.
   * @param <U>
   *          Type of the first {@link PTable}'s values
   * @param <V>
   *          Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right) {
    return innerJoin(left, right);
  }

  /**
   * Performs an inner join on the specified {@link PTable}s.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join">Inner
   *      Join</a>
   * @param left
   *          A PTable to perform an inner join on.
   * @param right
   *          A PTable to perform an inner join on.
   * @param <K>
   *          Type of the keys.
   * @param <U>
   *          Type of the first {@link PTable}'s values
   * @param <V>
   *          Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> innerJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new InnerJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
  }

  /**
   * Performs a left outer join on the specified {@link PTable}s.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join">Left
   *      Join</a>
   * @param left
   *          A PTable to perform an left join on. All of this PTable's entries
   *          will appear in the resulting PTable.
   * @param right
   *          A PTable to perform an left join on.
   * @param <K>
   *          Type of the keys.
   * @param <U>
   *          Type of the first {@link PTable}'s values
   * @param <V>
   *          Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> leftJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new LeftOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
  }

  /**
   * Performs a right outer join on the specified {@link PTable}s.
   * 
   * @see <a
   *      href="http://en.wikipedia.org/wiki/Join_(SQL)#Right_outer_join">Right
   *      Join</a>
   * @param left
   *          A PTable to perform an right join on.
   * @param right
   *          A PTable to perform an right join on. All of this PTable's entries
   *          will appear in the resulting PTable.
   * @param <K>
   *          Type of the keys.
   * @param <U>
   *          Type of the first {@link PTable}'s values
   * @param <V>
   *          Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> rightJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new RightOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
  }

  /**
   * Performs a full outer join on the specified {@link PTable}s.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join">Full
   *      Join</a>
   * @param left
   *          A PTable to perform an full join on.
   * @param right
   *          A PTable to perform an full join on.
   * @param <K>
   *          Type of the keys.
   * @param <U>
   *          Type of the first {@link PTable}'s values
   * @param <V>
   *          Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> fullJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new FullOuterJoinFn<K, U, V>(left.getKeyType(), left.getValueType()));
  }

  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinFn<K, U, V> joinFn) {
    PTypeFamily ptf = left.getTypeFamily();
    PGroupedTable<Pair<K, Integer>, Pair<U, V>> grouped = preJoin(left, right);
    PTableType<K, Pair<U, V>> ret = ptf
        .tableOf(left.getKeyType(), ptf.pairs(left.getValueType(), right.getValueType()));

    return grouped.parallelDo(joinFn.getJoinType() + grouped.getName(), joinFn, ret);
  }

  private static <K, U, V> PGroupedTable<Pair<K, Integer>, Pair<U, V>> preJoin(PTable<K, U> left, PTable<K, V> right) {
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
    optionsBuilder.partitionerClass(JoinUtils.getPartitionerClass(ptf));

    return (tag1.union(tag2)).groupByKey(optionsBuilder.build());
  }
}
