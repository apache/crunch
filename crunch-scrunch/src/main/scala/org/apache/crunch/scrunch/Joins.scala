/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.crunch.scrunch

import org.apache.crunch.{Pair => P}
import org.apache.crunch.lib.join._
import org.apache.crunch.lib.join.ShardedJoinStrategy.ShardingStrategy
import org.apache.crunch
import org.apache.crunch.fn.SwapFn

/**
 * An adapter for the JoinStrategy interface that works with Scrunch PTables.
 */
class ScrunchJoinStrategy[K, U, V](val delegate: JoinStrategy[K, U, V]) {
  def join(left: PTable[K, U], right: PTable[K, V], joinType: JoinType) = {
    val jres = delegate.join(left.native, right.native, joinType)
    val ptf = left.getTypeFamily()
    val ptype = ptf.tableOf(left.keyType(), ptf.tuple2(left.valueType(), right.valueType()))
    val inter = new PTable[K, P[U, V]](jres)
    inter.parallelDo(new SMapTableValuesFn[K, P[U, V], (U, V)] {
      def apply(x: P[U, V]) = (x.first(), x.second())
    }, ptype)
  }
}

private class ReverseJoinStrategy[K, U, V](val delegate: JoinStrategy[K, V, U]) extends JoinStrategy[K, U, V] {
  override def join(left: crunch.PTable[K, U], right: crunch.PTable[K, V], joinType: JoinType) = {
    val res: crunch.PTable[K, P[V, U]] =
      if (joinType == JoinType.LEFT_OUTER_JOIN) {
        delegate.join(right, left, JoinType.RIGHT_OUTER_JOIN)
      } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
        delegate.join(right, left, JoinType.LEFT_OUTER_JOIN)
      } else {
        delegate.join(right, left, joinType)
      }
    res.mapValues(new SwapFn[V, U](), SwapFn.ptype(res.getValueType))
  }
}

object ScrunchJoinStrategy {
  def apply[K, U, V](delegate: JoinStrategy[K, U, V]) = new ScrunchJoinStrategy[K, U, V](delegate)
}

class ScalaShardingStrategy[K](val f: K => Int) extends ShardingStrategy[K] {
  override def getNumShards(key: K): Int = f(key)
}

/**
 * Factory methods for the common JoinStrategy implementations in Crunch adapted to work with Scrunch
 * PTables.
 */
object Joins {

  /**
   * Strategy that implements a standard reduce-side join.
   *
   * @param numReducers the number of partitions to use to perform the shuffle
   */
  def default[K, U, V](numReducers: Int = -1) = {
    ScrunchJoinStrategy(new DefaultJoinStrategy[K, U, V](numReducers))
  }

  /**
   * Strategy for performing a mapside-join in which the left-side PTable will be optionally
   * materialized to disk and then loaded into memory while we stream over the contents right-side
   * PTable.
   *
   * @param materialize Whether to materialize the left side table before loading it into
   *                    memory, vs. performing any transformations it requires in-memory on the cluster
   */
  def mapside[K, U, V](materialize: Boolean = true) = {
    ScrunchJoinStrategy(new ReverseJoinStrategy[K, U, V](new MapsideJoinStrategy[K, V, U](materialize)))
  }

  /**
   * Join strategy that uses a bloom filter over the keys in the left hand PTable
   * @param numElements the expected number of unique keys
   * @param falsePositiveRate the acceptable false positive rate for the filter
   * @param delegate the underlying join strategy to use once the bloom filter is created
   */
  def bloom[K, U, V](numElements: Int, falsePositiveRate: Float = 0.05f,
                     delegate: ScrunchJoinStrategy[K, U, V] = default()) = {
    ScrunchJoinStrategy(new BloomFilterJoinStrategy[K, U, V](numElements, falsePositiveRate, delegate.delegate))
  }

  /**
   * Join strategy that shards the value associated with a single key across multiple reducers.
   * Useful when one of the PTables has a skewed key distribution, where lots of values are
   * associated with a small number of keys.
   * @param numShards number of shards to use with the default sharding strategy
   */
  def sharded[K, U, V](numShards: Int): ScrunchJoinStrategy[K, U, V] = {
    ScrunchJoinStrategy(new ShardedJoinStrategy[K, U, V](numShards))
  }

  /**
   * Sharded join that uses the given Scala function to determine the shard for each key.
   */
  def sharded[K, U, V](f: K => Int): ScrunchJoinStrategy[K, U, V] = {
    sharded[K, U, V](new ScalaShardingStrategy[K](f))
  }

  /**
   * Sharded join with a custom sharding strategy.
   * @param strategy the strategy to use
   */
  def sharded[K, U, V](strategy: ShardingStrategy[K]): ScrunchJoinStrategy[K, U, V] = {
    ScrunchJoinStrategy(new ShardedJoinStrategy[K, U, V](strategy))
  }
}
