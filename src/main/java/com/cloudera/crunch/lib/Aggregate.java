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

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.type.PTypeFamily;

/**
 * Methods for performing various types of aggregations over {@link PCollection}
 * instances.
 *
 */
public class Aggregate {

  /**
   * Returns a {@code PTable} that contains the unique elements of this
   * collection mapped to a count of their occurrences.
   */
  public static <S> PTable<S, Long> count(PCollection<S> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    return collect.parallelDo("Aggregate.count", new MapFn<S, Pair<S, Long>>() {
      @Override
      public Pair<S, Long> map(S input) {
        return Pair.of(input, 1L);
      }
    }, tf.tableOf(collect.getPType(), tf.longs()))
    .groupByKey()
    .combineValues(CombineFn.<S> SUM_LONGS());
  }
}
