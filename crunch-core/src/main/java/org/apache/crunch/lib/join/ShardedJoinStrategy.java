/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.crunch.lib.join;

import java.io.Serializable;
import java.util.Random;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;

/**
 * JoinStrategy that splits the key space up into shards.
 * <p>
 * This strategy is useful when there are multiple values per key on at least one side of the join,
 * and a large proportion of the values are mapped to a small number of keys.
 * <p>
 * Using this strategy will increase the number of keys being joined, but can increase performance
 * by spreading processing of a single key over multiple reduce groups.
 * <p>
 * A custom {@link ShardingStrategy} can be provided so that only certain keys are sharded, or 
 * keys can be sharded in accordance with how many values are mapped to them.
 */
public class ShardedJoinStrategy<K, U, V> implements JoinStrategy<K, U, V> {
  
  /**
   * Determines over how many shards a key will be split in a sharded join.
   * <p>
   * It is essential that implementations of this class are deterministic.
   */
  public static interface ShardingStrategy<K> extends Serializable {

    /**
     * Retrieve the number of shards over which the given key should be split.
     * @param key key for which shards are to be determined
     * @return number of shards for the given key, must be greater than 0
     */
    int getNumShards(K key);

  }
  
  
  private JoinStrategy<Pair<K, Integer>, U, V> wrappedJoinStrategy;
  private ShardingStrategy<K> shardingStrategy;
  
  /**
   * Instantiate with a constant number of shards to use for all keys.
   * 
   * @param numShards number of shards to use
   */
  public ShardedJoinStrategy(int numShards) {
    this(new ConstantShardingStrategy<K>(numShards));
  }

  /**
   * Instantiate with a constant number of shards to use for all keys.
   *
   * @param numShards number of shards to use
   * @param numReducers the amount of reducers to run the join with
   */
  public ShardedJoinStrategy(int numShards, int numReducers) {
    this(new ConstantShardingStrategy<K>(numShards), numReducers);
  }
  
  /**
   * Instantiate with a custom sharding strategy.
   * 
   * @param shardingStrategy strategy to be used for sharding
   */
  public ShardedJoinStrategy(ShardingStrategy<K> shardingStrategy) {
    this.wrappedJoinStrategy = new DefaultJoinStrategy<Pair<K, Integer>, U, V>();
    this.shardingStrategy = shardingStrategy;
  }

  /**
   * Instantiate with a custom sharding strategy and a specified number of reducers.
   *
   * @param shardingStrategy strategy to be used for sharding
   * @param numReducers the amount of reducers to run the join with
   */
  public ShardedJoinStrategy(ShardingStrategy<K> shardingStrategy, int numReducers) {
    if (numReducers < 1) {
      throw new IllegalArgumentException("Num reducers must be > 0, got " + numReducers);
    }
    this.wrappedJoinStrategy = new DefaultJoinStrategy<Pair<K, Integer>, U, V>(numReducers);
    this.shardingStrategy = shardingStrategy;
  }

  @Override
  public PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinType joinType) {
    
    if (joinType == JoinType.FULL_OUTER_JOIN || joinType == JoinType.LEFT_OUTER_JOIN) {
      throw new UnsupportedOperationException("Join type " + joinType + " not supported by ShardedJoinStrategy");
    }
    
    PTypeFamily ptf = left.getTypeFamily();
    PTableType<Pair<K, Integer>, U> shardedLeftType = ptf.tableOf(ptf.pairs(left.getKeyType(), ptf.ints()), left.getValueType());
    PTableType<Pair<K, Integer>, V> shardedRightType = ptf.tableOf(ptf.pairs(right.getKeyType(), ptf.ints()), right.getValueType());
    PTableType<K, Pair<U,V>> outputType = ptf.tableOf(left.getKeyType(), ptf.pairs(left.getValueType(), right.getValueType()));
    
    PTable<Pair<K,Integer>,U> shardedLeft = left.parallelDo("Pre-shard left", new PreShardLeftSideFn<K, U>(shardingStrategy), shardedLeftType);
    PTable<Pair<K,Integer>,V> shardedRight = right.parallelDo("Pre-shard right", new PreShardRightSideFn<K, V>(shardingStrategy), shardedRightType);

    PTable<Pair<K, Integer>, Pair<U, V>> shardedJoined = wrappedJoinStrategy.join(shardedLeft, shardedRight, joinType);
    
    return shardedJoined.parallelDo("Unshard", new UnshardFn<K, U, V>(), outputType);
  }

  private static class PreShardLeftSideFn<K, U> extends DoFn<Pair<K, U>, Pair<Pair<K, Integer>, U>> {

    private ShardingStrategy<K> shardingStrategy;

    public PreShardLeftSideFn(ShardingStrategy<K> shardingStrategy) {
      this.shardingStrategy = shardingStrategy;
    }

    @Override
    public void process(Pair<K, U> input, Emitter<Pair<Pair<K, Integer>, U>> emitter) {
      K key = input.first();
      int numShards = shardingStrategy.getNumShards(key);
      if (numShards < 1) {
        throw new IllegalArgumentException("Num shards must be > 0, got " + numShards + " for " + key);
      }
      for (int i = 0; i < numShards; i++) {
        emitter.emit(Pair.of(Pair.of(key, i), input.second()));
      }
    }

  }

  private static class PreShardRightSideFn<K, V> extends MapFn<Pair<K, V>, Pair<Pair<K, Integer>, V>> {

    private ShardingStrategy<K> shardingStrategy;
    private transient Random random;

    public PreShardRightSideFn(ShardingStrategy<K> shardingStrategy) {
      this.shardingStrategy = shardingStrategy;
    }
    
    @Override
    public void initialize() {
      random = new Random(getTaskAttemptID().getTaskID().getId());
    }

    @Override
    public Pair<Pair<K, Integer>, V> map(Pair<K, V> input) {
      K key = input.first();
      V value = input.second();
      int numShards = shardingStrategy.getNumShards(key);
      if (numShards < 1) {
        throw new IllegalArgumentException("Num shards must be > 0, got " + numShards + " for " + key);
      }
      
      return Pair.of(Pair.of(key, random.nextInt(numShards)), value);
    }

  }

  private static class UnshardFn<K, U, V> extends MapFn<Pair<Pair<K, Integer>, Pair<U, V>>, Pair<K, Pair<U, V>>> {

    @Override
    public Pair<K, Pair<U, V>> map(Pair<Pair<K, Integer>, Pair<U, V>> input) {
      return Pair.of(input.first().first(), input.second());
    }

  }
  
  /**
   * Sharding strategy that returns the same number of shards for all keys.
   */
  private static class ConstantShardingStrategy<K> implements ShardingStrategy<K> {

    private int numShards;

    public ConstantShardingStrategy(int numShards) {
      this.numShards = numShards;
    }
    
    @Override
    public int getNumShards(K key) {
      return numShards;
    }
    
  }

}
