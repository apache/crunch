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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.crunch.Aggregator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.materialize.pobject.FirstElementPObject;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.util.PartitionUtils;

import com.google.common.collect.Lists;

/**
 * Methods for performing various types of aggregations over {@link PCollection} instances.
 * 
 */
public class Aggregate {

  /**
   * Returns a {@code PTable} that contains the unique elements of this collection mapped to a count
   * of their occurrences.
   */
  public static <S> PTable<S, Long> count(PCollection<S> collect) {
    return count(collect, PartitionUtils.getRecommendedPartitions(collect));
  }

  /**
   * Returns a {@code PTable} that contains the unique elements of this collection mapped to a count
   * of their occurrences.
   */
  public static <S> PTable<S, Long> count(PCollection<S> collect, int numPartitions) {
    PTypeFamily tf = collect.getTypeFamily();
    return collect.parallelDo("Aggregate.count", new MapFn<S, Pair<S, Long>>() {
      public Pair<S, Long> map(S input) {
        return Pair.of(input, 1L);
      }
    }, tf.tableOf(collect.getPType(), tf.longs()))
        .groupByKey(numPartitions)
        .combineValues(Aggregators.SUM_LONGS());
  }
  
  /**
   * Returns the number of elements in the provided PCollection.
   * 
   * @param collect The PCollection whose elements should be counted.
   * @param <S> The type of the PCollection.
   * @return A {@code PObject} containing the number of elements in the {@code PCollection}.
   */
  public static <S> PObject<Long> length(PCollection<S> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    PTable<Integer, Long> countTable = collect
        .parallelDo("Aggregate.count", new MapFn<S, Pair<Integer, Long>>() {
          public Pair<Integer, Long> map(S input) {
            return Pair.of(1, 1L);
          }
          public void cleanup(Emitter<Pair<Integer, Long>> e) {
            e.emit(Pair.of(1, 0L));
          }
        }, tf.tableOf(tf.ints(), tf.longs()))
        .groupByKey(GroupingOptions.builder().numReducers(1).build())
        .combineValues(Aggregators.SUM_LONGS());
    PCollection<Long> count = countTable.values();
    return new FirstElementPObject<Long>(count, 0L);
  }

  public static class PairValueComparator<K, V> implements Comparator<Pair<K, V>> {
    private final boolean ascending;

    public PairValueComparator(boolean ascending) {
      this.ascending = ascending;
    }

    @Override
    public int compare(Pair<K, V> left, Pair<K, V> right) {
      int cmp = ((Comparable<V>) left.second()).compareTo(right.second());
      if (ascending) {
        return cmp;
      } else {
        return cmp == Integer.MIN_VALUE ? Integer.MAX_VALUE : -cmp;
      }
    }
  }

  public static class TopKFn<K, V> extends DoFn<Pair<K, V>, Pair<Integer, Pair<K, V>>> {

    private final int limit;
    private final boolean maximize;
    private final PType<Pair<K, V>> pairType;
    private transient PriorityQueue<Pair<K, V>> values;

    public TopKFn(int limit, boolean ascending, PType<Pair<K, V>> pairType) {
      this.limit = limit;
      this.maximize = ascending;
      this.pairType = pairType;
    }

    public void initialize() {
      this.values = new PriorityQueue<Pair<K, V>>(limit, new PairValueComparator<K, V>(maximize));
      pairType.initialize(getConfiguration());
    }

    public void process(Pair<K, V> input, Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      values.add(pairType.getDetachedValue(input));
      if (values.size() > limit) {
        values.poll();
      }
    }

    public void cleanup(Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      for (Pair<K, V> p : values) {
        emitter.emit(Pair.of(0, p));
      }
    }
  }

  public static class TopKCombineFn<K, V> extends CombineFn<Integer, Pair<K, V>> {

    private final int limit;
    private final boolean maximize;
    private PType<Pair<K, V>> pairType;

    public TopKCombineFn(int limit, boolean maximize, PType<Pair<K, V>> pairType) {
      this.limit = limit;
      this.maximize = maximize;
      this.pairType = pairType;
    }

    @Override
    public void initialize() {
      pairType.initialize(getConfiguration());
    }

    @Override
    public void process(Pair<Integer, Iterable<Pair<K, V>>> input,
        Emitter<Pair<Integer, Pair<K, V>>> emitter) {
      Comparator<Pair<K, V>> cmp = new PairValueComparator<K, V>(maximize);
      PriorityQueue<Pair<K, V>> queue = new PriorityQueue<Pair<K, V>>(limit, cmp);
      for (Pair<K, V> pair : input.second()) {
        queue.add(pairType.getDetachedValue(pair));
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

  /**
   * Selects the top N pairs from the given table, with sorting being performed on the values (i.e. the second
   * value in the pair) of the table.
   *
   * @param ptable table containing the pairs from which the top N is to be selected
   * @param limit number of top elements to select
   * @param maximize if true, the maximum N values from the table will be selected, otherwise the minimal
   *                 N values will be selected
   * @return table containing the top N values from the incoming table
   */
  public static <K, V> PTable<K, V> top(PTable<K, V> ptable, int limit, boolean maximize) {
    PTypeFamily ptf = ptable.getTypeFamily();
    PTableType<K, V> base = ptable.getPTableType();
    PType<Pair<K, V>> pairType = ptf.pairs(base.getKeyType(), base.getValueType());
    PTableType<Integer, Pair<K, V>> inter = ptf.tableOf(ptf.ints(), pairType);
    return ptable.parallelDo("top" + limit + "map", new TopKFn<K, V>(limit, maximize, pairType), inter)
        .groupByKey(1).combineValues(new TopKCombineFn<K, V>(limit, maximize, pairType))
        .parallelDo("top" + limit + "reduce", new DoFn<Pair<Integer, Pair<K, V>>, Pair<K, V>>() {
          public void process(Pair<Integer, Pair<K, V>> input, Emitter<Pair<K, V>> emitter) {
            emitter.emit(input.second());
          }
        }, base);
  }

  /**
   * Returns the largest numerical element from the input collection.
   */
  public static <S> PObject<S> max(PCollection<S> collect) {
    Class<S> clazz = collect.getPType().getTypeClass();
    if (!clazz.isPrimitive() && !Comparable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Can only get max for Comparable elements, not for: "
          + collect.getPType().getTypeClass());
    }
    PTypeFamily tf = collect.getTypeFamily();
    PCollection<S> maxCollect = PTables.values(collect
        .parallelDo("max", new DoFn<S, Pair<Boolean, S>>() {
          private transient S max = null;

          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (max == null || ((Comparable<S>) max).compareTo(input) < 0) {
              max = input;
            }
          }

          public void cleanup(Emitter<Pair<Boolean, S>> emitter) {
            if (max != null) {
              emitter.emit(Pair.of(true, max));
            }
          }
        }, tf.tableOf(tf.booleans(), collect.getPType())).groupByKey(1)
        .combineValues(new CombineFn<Boolean, S>() {
          public void process(Pair<Boolean, Iterable<S>> input, Emitter<Pair<Boolean, S>> emitter) {
            S max = null;
            for (S v : input.second()) {
              if (max == null || ((Comparable<S>) max).compareTo(v) < 0) {
                max = v;
              }
            }
            emitter.emit(Pair.of(input.first(), max));
          }
        }));
    return new FirstElementPObject<S>(maxCollect);
  }

  /**
   * Returns the smallest numerical element from the input collection.
   */
  public static <S> PObject<S> min(PCollection<S> collect) {
    Class<S> clazz = collect.getPType().getTypeClass();
    if (!clazz.isPrimitive() && !Comparable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Can only get min for Comparable elements, not for: "
          + collect.getPType().getTypeClass());
    }
    PTypeFamily tf = collect.getTypeFamily();
    PCollection<S> minCollect = PTables.values(collect
        .parallelDo("min", new DoFn<S, Pair<Boolean, S>>() {
          private transient S min = null;

          public void process(S input, Emitter<Pair<Boolean, S>> emitter) {
            if (min == null || ((Comparable<S>) min).compareTo(input) > 0) {
              min = input;
            }
          }

          public void cleanup(Emitter<Pair<Boolean, S>> emitter) {
            if (min != null) {
              emitter.emit(Pair.of(false, min));
            }
          }
        }, tf.tableOf(tf.booleans(), collect.getPType())).groupByKey(1)
        .combineValues(new CombineFn<Boolean, S>() {
          public void process(Pair<Boolean, Iterable<S>> input, Emitter<Pair<Boolean, S>> emitter) {
            S min = null;
            for (S v : input.second()) {
              if (min == null || ((Comparable<S>) min).compareTo(v) > 0) {
                min = v;
              }
            }
            emitter.emit(Pair.of(input.first(), min));
          }
        }));
    return new FirstElementPObject<S>(minCollect);
  }

  public static <K, V> PTable<K, Collection<V>> collectValues(PTable<K, V> collect) {
    PTypeFamily tf = collect.getTypeFamily();
    final PType<V> valueType = collect.getValueType();
    return collect.groupByKey().mapValues("collect",
        new MapFn<Iterable<V>, Collection<V>>() {
          @Override
          public void initialize() {
            valueType.initialize(getConfiguration());
          }

          public Collection<V> map(Iterable<V> values) {
            List<V> collected = Lists.newArrayList();
            for (V value : values) {
              collected.add(valueType.getDetachedValue(value));
            }
            return collected;
          }
        }, tf.collections(collect.getValueType()));
  }
  
  public static <S> PCollection<S> aggregate(PCollection<S> collect, Aggregator<S> aggregator) {
    PTypeFamily tf = collect.getTypeFamily();
    return collect.parallelDo("Aggregate.aggregator", new MapFn<S, Pair<Boolean, S>>() {
      public Pair<Boolean, S> map(S input) {
        return Pair.of(false, input);
      }
    }, tf.tableOf(tf.booleans(), collect.getPType()))
    .groupByKey(1)
    .combineValues(aggregator)
    .values();
  }
}
