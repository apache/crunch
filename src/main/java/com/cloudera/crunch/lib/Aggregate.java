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

import java.util.Collection;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.fn.MapValuesFn;
import com.cloudera.crunch.type.PTypeFamily;
import com.google.common.collect.Lists;

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
  
  /**
   * Returns the largest numerical element from the input collection.
   */
  public static <S extends Number> PCollection<S> max(PCollection<S> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    return PTables.values(
        collect.parallelDo(new DoFn<S, Pair<Boolean, S>>() {
          private transient S max = null;
          
          @Override
          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (max == null || max.doubleValue() < input.doubleValue()) {
              max = input;
            }
          }
          
          @Override
          public void cleanup(Emitter<Pair<Boolean, S>> emitter) {
            if (max != null) {
              emitter.emit(Pair.of(true, max));
            }
          }
        }, tf.tableOf(tf.booleans(), collect.getPType()))
        .groupByKey().combineValues(new CombineFn<Boolean, S>() {
          @Override
          public void process(Pair<Boolean, Iterable<S>> input,
              Emitter<Pair<Boolean, S>> emitter) {
            S max = null;
            for (S v : input.second()) {
              if (max == null || max.doubleValue() < v.doubleValue()) {
                max = v;
              }
            }
            emitter.emit(Pair.of(input.first(), max));
          } }));
  }
  
  /**
   * Returns the smallest numerical element from the input collection.
   */
  public static <S extends Number> PCollection<S> min(PCollection<S> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    return PTables.values(
        collect.parallelDo(new DoFn<S, Pair<Boolean, S>>() {
          private transient S min = null;
          
          @Override
          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (min == null || min.doubleValue() > input.doubleValue()) {
              min = input;
            }
          }
          
          @Override
          public void cleanup(Emitter<Pair<Boolean, S>> emitter) {
            if (min != null) {
              emitter.emit(Pair.of(false, min));
            }
          }
        }, tf.tableOf(tf.booleans(), collect.getPType()))
        .groupByKey().combineValues(new CombineFn<Boolean, S>() {
          @Override
          public void process(Pair<Boolean, Iterable<S>> input,
              Emitter<Pair<Boolean, S>> emitter) {
            S min = null;
            for (S v : input.second()) {
              if (min == null || min.doubleValue() > v.doubleValue()) {
                min = v;
              }
            }
            emitter.emit(Pair.of(input.first(), min));
          } }));
  }
  
  public static <K, V> PTable<K, Collection<V>> collectValues(PTable<K, V> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    return collect.groupByKey().parallelDo(new MapValuesFn<K, Iterable<V>, Collection<V>>() {
      @Override
      public Collection<V> map(Iterable<V> v) {
        return Lists.newArrayList(v);
      }
    }, tf.tableOf(collect.getKeyType(), tf.collections(collect.getValueType())));
        
  }
}
