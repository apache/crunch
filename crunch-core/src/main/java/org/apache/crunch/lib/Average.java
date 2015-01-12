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

import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTypeFamily;

import static org.apache.crunch.fn.Aggregators.SUM_DOUBLES;
import static org.apache.crunch.fn.Aggregators.SUM_LONGS;
import static org.apache.crunch.fn.Aggregators.pairAggregator;

public class Average {

  /**
   * Calculate the mean average value by key for a table with numeric values.
   * @param table PTable of (key, value) pairs to operate on
   * @param <K> Key type, can be any type
   * @param <V> Value type, must be numeric (ie. extend java.lang.Number)
   * @return PTable&lt;K, Double&gt; of (key, mean(values)) pairs
   */
  public static <K, V extends Number> PTable<K, Double> meanValue(PTable<K, V> table) {
    PTypeFamily ptf = table.getTypeFamily();
    PTable<K, Pair<Double, Long>> withCounts = table.mapValues(new MapFn<V, Pair<Double, Long>>() {

      @Override
      public Pair<Double, Long> map(V input) {
        return Pair.of(input.doubleValue(), 1L);
      }
    }, ptf.pairs(ptf.doubles(), ptf.longs()));
    PGroupedTable<K, Pair<Double, Long>> grouped = withCounts.groupByKey();

    return grouped.combineValues(pairAggregator(SUM_DOUBLES(), SUM_LONGS()))
            .mapValues(new MapFn<Pair<Double, Long>, Double>() {
             @Override
             public Double map(Pair<Double, Long> input) {
               return input.first() / input.second();
             }
            }, ptf.doubles());
  }

}
