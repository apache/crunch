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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.fn.MapValuesFn;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
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
  
  public static class PairValueComparator<K, V> implements Comparator<Pair<K, V>> {
    private final boolean ascending;
    
    public PairValueComparator(boolean ascending) {
      this.ascending = ascending;
    }
    
    @Override
    public int compare(Pair<K, V> left, Pair<K, V> right) {
      int cmp = ((Comparable<V>)left.second()).compareTo(right.second());
      return ascending ? cmp : -cmp;
    }
  }
  
  public static class TopKFn<K, V> extends DoFn<Pair<K, V>, Pair<Integer, Pair<K, V>>> {
    private final int limit;
    private final boolean maximize;
    private transient PriorityQueue<Pair<K, V>> values;
    
    public TopKFn(int limit, boolean ascending) {
      this.limit = limit;
      this.maximize = ascending;
    }
    
    @Override
    public void initialize() {
      this.values = new PriorityQueue<Pair<K, V>>(limit, new PairValueComparator<K, V>(maximize));
    }
    
    @Override
    public void process(Pair<K, V> input, Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      values.add(input);
      if (values.size() > limit) {
        values.poll();
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      for (Pair<K, V> p : values) {
        emitter.emit(Pair.of(0, p));
      }
    }
  }
  
  public static class TopKCombineFn<K, V> extends CombineFn<Integer, Pair<K, V>> {

    private final int limit;
    private final boolean maximize;
    
    public TopKCombineFn(int limit, boolean maximize) {
      this.limit = limit;
      this.maximize = maximize;
    }
    
    @Override
    public void process(Pair<Integer, Iterable<Pair<K, V>>> input,
        Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      Comparator<Pair<K, V>> cmp = new PairValueComparator<K, V>(maximize);
      PriorityQueue<Pair<K, V>> queue = new PriorityQueue<Pair<K, V>>(limit, cmp);
      for (Pair<K, V> pair : input.second()) {
        queue.add(pair);
        if (queue.size() > limit) {
          queue.poll();
        }
      }
      
      List<Pair<K, V>> values = Lists.newArrayList(queue);
      Collections.sort(values, cmp);
      for (int i = values.size() - 1; i >= 0; i--) {
        emitter.emit(Pair.of(0, values.get(i)));
      }
    }
  }
  
  public static <K, V> PTable<K, V> top(PTable<K, V> ptable, int limit, boolean maximize) {
    PTypeFamily ptf = ptable.getTypeFamily();
    PTableType<K, V> base = ptable.getPTableType();
    PType<Pair<K, V>> pairType = ptf.pairs(base.getKeyType(), base.getValueType());
    PTableType<Integer, Pair<K, V>> inter = ptf.tableOf(ptf.ints(), pairType);
    return ptable.parallelDo("top" + limit + "map", new TopKFn<K, V>(limit, maximize), inter)
        .groupByKey(1)
        .combineValues(new TopKCombineFn<K, V>(limit, maximize))
        .parallelDo("top" + limit + "reduce", new DoFn<Pair<Integer, Pair<K, V>>, Pair<K, V>>() {
          @Override
          public void process(Pair<Integer, Pair<K, V>> input,
              Emitter<Pair<K, V>> emitter) {
            emitter.emit(input.second()); 
          }
        }, base);
  }
  
  /**
   * Returns the largest numerical element from the input collection.
   */
  public static <S> PCollection<S> max(PCollection<S> collect) {
	Class<S> clazz = collect.getPType().getTypeClass();
	if (!clazz.isPrimitive() && !Comparable.class.isAssignableFrom(clazz)) {
	  throw new IllegalArgumentException(
	      "Can only get max for Comparable elements, not for: " + collect.getPType().getTypeClass());
	}
    PTypeFamily tf = collect.getTypeFamily();
    return PTables.values(
        collect.parallelDo("max", new DoFn<S, Pair<Boolean, S>>() {
          private transient S max = null;
          
          @Override
          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (max == null || ((Comparable<S>) max).compareTo(input) < 0) {
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
        .groupByKey(1).combineValues(new CombineFn<Boolean, S>() {
          @Override
          public void process(Pair<Boolean, Iterable<S>> input,
              Emitter<Pair<Boolean, S>> emitter) {
            S max = null;
            for (S v : input.second()) {
              if (max == null || ((Comparable<S>) max).compareTo(v) < 0) {
                max = v;
              }
            }
            emitter.emit(Pair.of(input.first(), max));
          } }));
  }
  
  /**
   * Returns the smallest numerical element from the input collection.
   */
  public static <S> PCollection<S> min(PCollection<S> collect) {
	Class<S> clazz = collect.getPType().getTypeClass();
	if (!clazz.isPrimitive() && !Comparable.class.isAssignableFrom(clazz)) {
	  throw new IllegalArgumentException(
	      "Can only get min for Comparable elements, not for: " + collect.getPType().getTypeClass());
	}
    PTypeFamily tf = collect.getTypeFamily();
    return PTables.values(
        collect.parallelDo("min", new DoFn<S, Pair<Boolean, S>>() {
          private transient S min = null;
          
          @Override
          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (min == null || ((Comparable<S>) min).compareTo(input) > 0) {
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
              if (min == null || ((Comparable<S>) min).compareTo(v) > 0) {
                min = v;
              }
            }
            emitter.emit(Pair.of(input.first(), min));
          } }));
  }
  
  public static <K, V> PTable<K, Collection<V>> collectValues(PTable<K, V> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    return collect.groupByKey().parallelDo("collect", new MapValuesFn<K, Iterable<V>, Collection<V>>() {
      @Override
      public Collection<V> map(Iterable<V> v) {
        return Lists.newArrayList(v);
      }
    }, tf.tableOf(collect.getKeyType(), tf.collections(collect.getValueType())));  
  }
}
