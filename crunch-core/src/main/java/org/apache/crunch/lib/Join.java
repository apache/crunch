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

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

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
    return new DefaultJoinStrategy<K, U, V>().join(left, right, JoinType.INNER_JOIN);
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
    return new DefaultJoinStrategy<K, U, V>().join(left, right, JoinType.LEFT_OUTER_JOIN);
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
    return new DefaultJoinStrategy<K, U, V>().join(left, right, JoinType.RIGHT_OUTER_JOIN);
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
    return new DefaultJoinStrategy<K, U, V>().join(left, right, JoinType.FULL_OUTER_JOIN);
  }

  
}
