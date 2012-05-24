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
package com.cloudera.crunch.lib;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.lib.join.FullOuterJoinFn;
import com.cloudera.crunch.lib.join.InnerJoinFn;
import com.cloudera.crunch.lib.join.JoinFn;
import com.cloudera.crunch.lib.join.JoinUtils;
import com.cloudera.crunch.lib.join.LeftOuterJoinFn;
import com.cloudera.crunch.lib.join.RightOuterJoinFn;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PTypeFamily;

/**
 * Utilities for joining multiple {@code PTable} instances based on a common lastKey.
 */
public class Join {
  /**
   * Performs an inner join on the specified {@link PTable}s.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join">Inner Join</a>
   * @param left A PTable to perform an inner join on.
   * @param right A PTable to perform an inner join on.
   * @param <K> Type of the keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right) {
    return innerJoin(left, right);
  }

  /**
   * Performs an inner join on the specified {@link PTable}s.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join">Inner Join</a>
   * @param left A PTable to perform an inner join on.
   * @param right A PTable to perform an inner join on.
   * @param <K> Type of the keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> innerJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new InnerJoinFn<K, U, V>());
  }

  /**
   * Performs a left outer join on the specified {@link PTable}s.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join">Left Join</a>
   * @param left A PTable to perform an left join on. All of this PTable's entries will appear
   *     in the resulting PTable.
   * @param right A PTable to perform an left join on.
   * @param <K> Type of the keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> leftJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new LeftOuterJoinFn<K, U, V>());
  }

  /**
   * Performs a right outer join on the specified {@link PTable}s.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Right_outer_join">Right Join</a>
   * @param left A PTable to perform an right join on.
   * @param right A PTable to perform an right join on. All of this PTable's entries will appear
   *     in the resulting PTable.
   * @param <K> Type of the keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> rightJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new RightOuterJoinFn<K, U, V>());
  }

  /**
   * Performs a full outer join on the specified {@link PTable}s.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join">Full Join</a>
   * @param left A PTable to perform an full join on.
   * @param right A PTable to perform an full join on.
   * @param <K> Type of the keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result.
   */
  public static <K, U, V> PTable<K, Pair<U, V>> fullJoin(PTable<K, U> left, PTable<K, V> right) {
    return join(left, right, new FullOuterJoinFn<K, U, V>());
  }

  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right,
      JoinFn<K, U, V> joinFn) {
    PTypeFamily ptf = left.getTypeFamily();
    PGroupedTable<Pair<K, Integer>, Pair<U, V>> grouped = preJoin(left, right);
    PTableType<K, Pair<U, V>> ret = ptf.tableOf(left.getKeyType(),
        ptf.pairs(left.getValueType(), right.getValueType()));

    return grouped.parallelDo(joinFn.getJoinType() + grouped.getName(), joinFn, ret);
  }

  private static <K, U, V> PGroupedTable<Pair<K, Integer>, Pair<U, V>> preJoin(
      PTable<K, U> left, PTable<K, V> right) {
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
