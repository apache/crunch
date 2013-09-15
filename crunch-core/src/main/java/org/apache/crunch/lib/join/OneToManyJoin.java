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

import java.io.Serializable;

import javax.annotation.Nullable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.PType;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Optimized join for situations where exactly one value is being joined with 
 * any other number of values based on a common key.
 */
public class OneToManyJoin {

  /**
   * Performs a join on two tables, where the left table only contains a single
   * value per key.
   * <p>
   * This method accepts a DoFn, which is responsible for converting the single
   * left-side value and the iterable of right-side values into output values.
   * <p>
   * This method of joining is useful when there is a single context value that
   * contains a large number of related values, and all related values must be
   * brought together, with the quantity of the right-side values being too big
   * to fit in memory.
   * <p>
   * If there are multiple values for the same key in the left-side table, only
   * a single one will be used.
   * 
   * @param left left-side table to join
   * @param right right-side table to join
   * @param postProcessFn DoFn to process the results of the join
   * @param ptype type of the output of the postProcessFn
   * @return the post-processed output of the join
   */
  public static <K, U, V, T> PCollection<T> oneToManyJoin(PTable<K, U> left, PTable<K, V> right,
                                                          DoFn<Pair<U, Iterable<V>>, T> postProcessFn, PType<T> ptype) {
    return oneToManyJoin(left, right, postProcessFn, ptype, -1);
  }

  /**
   * Supports a user-specified number of reducers for the one-to-many join.
   *
   * @param left left-side table to join
   * @param right right-side table to join
   * @param postProcessFn DoFn to process the results of the join
   * @param ptype type of the output of the postProcessFn
   * @param numReducers The number of reducers to use
   * @return the post-processed output of the join
   */
  public static <K, U, V, T> PCollection<T> oneToManyJoin(PTable<K, U> left, PTable<K, V> right,
      DoFn<Pair<U, Iterable<V>>, T> postProcessFn, PType<T> ptype, int numReducers) {

    PGroupedTable<Pair<K, Integer>, Pair<U, V>> grouped = DefaultJoinStrategy.preJoin(left, right, numReducers);
    return grouped.parallelDo("One to many join " + grouped.getName(),
        new OneToManyJoinFn<K, U, V, T>(left.getValueType(), postProcessFn), ptype);
  }

  /**
   * Handles post-processing the output of {@link Join#oneToManyJoin}.
   */
  static class OneToManyJoinFn<K, U, V, T> extends DoFn<Pair<Pair<K, Integer>, Iterable<Pair<U, V>>>, T> {

    private PType<U> leftValueType;
    private DoFn<Pair<U, Iterable<V>>, T> postProcessFn;
    private SecondElementFunction<U, V> secondElementFunction;
    private K currentKey;
    private U leftValue;

    public OneToManyJoinFn(PType<U> leftValueType, DoFn<Pair<U, Iterable<V>>, T> postProcessFn) {
      this.leftValueType = leftValueType;
      this.postProcessFn = postProcessFn;
      this.secondElementFunction = new SecondElementFunction<U, V>();
    }

    @Override
    public void initialize() {
      super.initialize();
      postProcessFn.initialize();
      leftValueType.initialize(getConfiguration());
    }
    
    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      super.setContext(context);
      postProcessFn.setContext(context);
    }

    @Override
    public void process(Pair<Pair<K, Integer>, Iterable<Pair<U, V>>> input, Emitter<T> emitter) {
      Pair<K, Integer> keyPair = input.first();
      if (keyPair.second() == 0) {
        leftValue = leftValueType.getDetachedValue(input.second().iterator().next().first());
        currentKey = input.first().first();
      } else if (keyPair.second() == 1 && input.first().first().equals(currentKey)) {
        postProcessFn.process(Pair.of(leftValue, wrapIterable(input.second())), emitter);
        leftValue = null;
      }
    }

    private Iterable<V> wrapIterable(Iterable<Pair<U, V>> input) {
      return Iterables.transform(input, secondElementFunction);
    }

    private static class SecondElementFunction<U, V> implements Function<Pair<U, V>, V>, Serializable {

      @Override
      public V apply(@Nullable Pair<U, V> input) {
        return input.second();
      }

    }
  }
}
