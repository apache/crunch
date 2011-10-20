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

package com.cloudera.crunch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.util.Tuples;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A special {@link DoFn} implementation that converts an {@link Iterable}
 * of values into a single value. If a {@code CombineFn} instance is used
 * on a {@link PGroupedTable}, the function will be applied to the output
 * of the map stage before the data is passed to the reducer, which can
 * improve the runtime of certain classes of jobs.
 *
 */
public abstract class CombineFn<S, T> extends DoFn<Pair<S, Iterable<T>>, Pair<S, T>> {

  public static interface Aggregator<T> extends Serializable {
    /**
     * Clears the internal state of this Aggregator and prepares it for the values associated
     * with the next key.
     */
    void reset();

    /**
     * Incorporate the given value into the aggregate state maintained by this instance.
     */
    void update(T value);
    
    /**
     * Returns the current aggregated state of this instance.
     */
    Iterable<T> results();    
  }
  
  /**
   * A {@code CombineFn} that delegates all of the actual work to an {@code Aggregator}
   * instance.
   */
  public static class AggregatorCombineFn<K, V> extends CombineFn<K, V> {
    private final Aggregator<V> aggregator;
    
    public AggregatorCombineFn(Aggregator<V> aggregator) {
      this.aggregator = aggregator;
    }
    
    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      aggregator.reset();
      for (V v : input.second()) {
        aggregator.update(v);
      }
      for (V v : aggregator.results()) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }    
  }
  
  private static abstract class TupleAggregator<T> implements Aggregator<T> {
    private final List<Aggregator<Object>> aggregators;
    
    public TupleAggregator(Aggregator<?>...aggregators) {
      this.aggregators = Lists.newArrayList();
      for (Aggregator<?> a : aggregators) {
        this.aggregators.add((Aggregator<Object>) a);
      }
    }
    
    @Override
    public void reset() {
      for (Aggregator<?> a : aggregators) {
        a.reset();
      }
    }
    
    protected void updateTuple(Tuple t) {
      for (int i = 0; i < aggregators.size(); i++) {
        aggregators.get(i).update(t.get(i));
      }
    }
    
    protected Iterable<Object> results(int index) {
      return aggregators.get(index).results();
    }
  }
  
  public static class PairAggregator<V1, V2> extends TupleAggregator<Pair<V1, V2>> {
    public PairAggregator(Aggregator<V1> a1, Aggregator<V2> a2) {
      super(a1, a2);
    }
    
    @Override
    public void update(Pair<V1, V2> value) {
      updateTuple(value);
    }
    
    @Override
    public Iterable<Pair<V1, V2>> results() {
      return new Tuples.PairIterable<V1, V2>((Iterable<V1>) results(0), (Iterable<V2>) results(1));
    }
  }
  
  public static class TripAggregator<A, B, C> extends TupleAggregator<Tuple3<A, B, C>> {
    public TripAggregator(Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3) {
      super(a1, a2, a3);
    }
    
    @Override
    public void update(Tuple3<A, B, C> value) {
      updateTuple(value);
    }
    
    @Override
    public Iterable<Tuple3<A, B, C>> results() {
      return new Tuples.TripIterable<A, B, C>((Iterable<A>) results(0),
          (Iterable<B>) results(1), (Iterable<C>) results(2));
    }
  }

  public static class QuadAggregator<A, B, C, D> extends TupleAggregator<Tuple4<A, B, C, D>> {
    public QuadAggregator(Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3, Aggregator<D> a4) {
      super(a1, a2, a3, a4);
    }
    
    @Override
    public void update(Tuple4<A, B, C, D> value) {
      updateTuple(value);
    }
    
    @Override
    public Iterable<Tuple4<A, B, C, D>> results() {
      return new Tuples.QuadIterable<A, B, C, D>((Iterable<A>) results(0),
          (Iterable<B>) results(1), (Iterable<C>) results(2), (Iterable<D>) results(3));
    }
  }
  
  public static class TupleNAggregator extends TupleAggregator<TupleN> {
    private final int size;
    
    public TupleNAggregator(Aggregator<?>... aggregators) {
      super(aggregators);
      size = aggregators.length;
    }
    
    @Override
    public void update(TupleN value) {
      updateTuple(value);
    }

    @Override
    public Iterable<TupleN> results() {
      Iterable[] iterables = new Iterable[size];
      for (int i = 0; i < size; i++) {
        iterables[i] = results(i);
      }
      return new Tuples.TupleNIterable(iterables);
    }
    
  }
  
  public static final <K, V> CombineFn<K, V> aggregator(Aggregator<V> aggregator) {
    return new AggregatorCombineFn<K, V>(aggregator);
  }
  
  public static final <K, V1, V2> CombineFn<K, Pair<V1, V2>> pairAggregator(
      Aggregator<V1> a1, Aggregator<V2> a2) {
    return aggregator(new PairAggregator<V1, V2>(a1, a2));
  }
  
  public static final <K, A, B, C> CombineFn<K, Tuple3<A, B, C>> tripAggregator(
      Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3) {
    return aggregator(new TripAggregator<A, B, C>(a1, a2, a3));
  }

  public static final <K, A, B, C, D> CombineFn<K, Tuple4<A, B, C, D>> quadAggregator(
      Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3, Aggregator<D> a4) {
    return aggregator(new QuadAggregator<A, B, C, D>(a1, a2, a3, a4));
  }

  public static final <K> CombineFn<K, TupleN> tupleAggregator(Aggregator<?>... aggregators) {
    return aggregator(new TupleNAggregator(aggregators));
  }
  
  public static final <K> CombineFn<K, Long> SUM_LONGS() {
    return aggregator(SUM_LONGS);
  }

  public static final <K> CombineFn<K, Integer> SUM_INTS() {
    return aggregator(SUM_INTS);
  }

  public static final <K> CombineFn<K, Float> SUM_FLOATS() {
    return aggregator(SUM_FLOATS);
  }

  public static final <K> CombineFn<K, Double> SUM_DOUBLES() {
    return aggregator(SUM_DOUBLES);
  }
  
  public static final <K> CombineFn<K, Long> MAX_LONGS() {
    return aggregator(MAX_LONGS);
  }

  public static final <K> CombineFn<K, Long> MAX_LONGS(int n) {
    return aggregator(new MaxNAggregator<Long>(n));
  }
  
  public static final <K> CombineFn<K, Integer> MAX_INTS() {
    return aggregator(MAX_INTS);
  }

  public static final <K> CombineFn<K, Integer> MAX_INTS(int n) {
    return aggregator(new MaxNAggregator<Integer>(n));
  }

  public static final <K> CombineFn<K, Float> MAX_FLOATS() {
    return aggregator(MAX_FLOATS);
  }

  public static final <K> CombineFn<K, Float> MAX_FLOATS(int n) {
    return aggregator(new MaxNAggregator<Float>(n));
  }

  public static final <K> CombineFn<K, Double> MAX_DOUBLES() {
    return aggregator(MAX_DOUBLES);
  }
  
  public static final <K> CombineFn<K, Double> MAX_DOUBLES(int n) {
    return aggregator(new MaxNAggregator<Double>(n));
  }
  
  public static final <K> CombineFn<K, Long> MIN_LONGS() {
    return aggregator(MIN_LONGS);
  }

  public static final <K> CombineFn<K, Long> MIN_LONGS(int n) {
    return aggregator(new MinNAggregator<Long>(n));
  }

  public static final <K> CombineFn<K, Integer> MIN_INTS() {
    return aggregator(MIN_INTS);
  }

  public static final <K> CombineFn<K, Integer> MIN_INTS(int n) {
    return aggregator(new MinNAggregator<Integer>(n));
  }
  
  public static final <K> CombineFn<K, Float> MIN_FLOATS() {
    return aggregator(MIN_FLOATS);
  }

  public static final <K> CombineFn<K, Float> MIN_FLOATS(int n) {
    return aggregator(new MinNAggregator<Float>(n));
  }
  
  public static final <K> CombineFn<K, Double> MIN_DOUBLES() {
    return aggregator(MIN_DOUBLES);
  }

  public static final <K> CombineFn<K, Double> MIN_DOUBLES(int n) {
    return aggregator(new MinNAggregator<Double>(n));
  }
  
  public static final <K, V> CombineFn<K, V> FIRST_N(int n) {
    return aggregator(new FirstNAggregator<V>(n));
  }

  public static final <K, V> CombineFn<K, V> LAST_N(int n) {
    return aggregator(new LastNAggregator<V>(n));
  }
  
  public static Aggregator<Long> SUM_LONGS = new Aggregator<Long>() {
    private long sum = 0;
    
    @Override
    public void reset() {
      sum = 0;
    }

    @Override
    public void update(Long next) {
      sum += next;
    }
    
    @Override
    public Iterable<Long> results() {
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Integer> SUM_INTS = new Aggregator<Integer>() {
    private int sum = 0;
    
    @Override
    public void reset() {
      sum = 0;
    }

    @Override
    public void update(Integer next) {
      sum += next;
    }
    
    @Override
    public Iterable<Integer> results() {
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Float> SUM_FLOATS = new Aggregator<Float>() {
    private float sum = 0;
    
    @Override
    public void reset() {
      sum = 0f;
    }

    @Override
    public void update(Float next) {
      sum += next;
    }
    
    @Override
    public Iterable<Float> results() {
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Double> SUM_DOUBLES = new Aggregator<Double>() {
    private double sum = 0;
    
    @Override
    public void reset() {
      sum = 0f;
    }

    @Override
    public void update(Double next) {
      sum += next;
    }
    
    @Override
    public Iterable<Double> results() {
      return ImmutableList.of(sum);
    }
  };

  public static Aggregator<Long> MAX_LONGS = new Aggregator<Long>() {
    private Long max = null;
    
    @Override
    public void reset() {
      max = null;
    }
    
    @Override
    public void update(Long next) {
      if (max == null || max < next) {
        max = next;
      }
    }
    
    @Override
    public Iterable<Long> results() {
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Integer> MAX_INTS = new Aggregator<Integer>() {
    private Integer max = null;
    
    @Override
    public void reset() {
      max = null;
    }
    
    @Override
    public void update(Integer next) {
      if (max == null || max < next) {
        max = next;
      }
    }
    
    @Override
    public Iterable<Integer> results() {
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Float> MAX_FLOATS = new Aggregator<Float>() {
    private Float max = null;
    
    @Override
    public void reset() {
      max = null;
    }
    
    @Override
    public void update(Float next) {
      if (max == null || max < next) {
        max = next;
      }
    }
    
    @Override
    public Iterable<Float> results() {
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Double> MAX_DOUBLES = new Aggregator<Double>() {
    private Double max = null;
    
    @Override
    public void reset() {
      max = null;
    }
    
    @Override
    public void update(Double next) {
      if (max == null || max < next) {
        max = next;
      }
    }
    
    @Override
    public Iterable<Double> results() {
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Long> MIN_LONGS = new Aggregator<Long>() {
    private Long min = null;
    
    @Override
    public void reset() {
      min = null;
    }
    
    @Override
    public void update(Long next) {
      if (min == null || min > next) {
        min = next;
      }
    }
    
    @Override
    public Iterable<Long> results() {
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Integer> MIN_INTS = new Aggregator<Integer>() {
    private Integer min = null;
    
    @Override
    public void reset() {
      min = null;
    }
    
    @Override
    public void update(Integer next) {
      if (min == null || min > next) {
        min = next;
      }
    }
    
    @Override
    public Iterable<Integer> results() {
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Float> MIN_FLOATS = new Aggregator<Float>() {
    private Float min = null;
    
    @Override
    public void reset() {
      min = null;
    }
    
    @Override
    public void update(Float next) {
      if (min == null || min > next) {
        min = next;
      }
    }
    
    @Override
    public Iterable<Float> results() {
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Double> MIN_DOUBLES = new Aggregator<Double>() {
    private Double min = null;
    
    @Override
    public void reset() {
      min = null;
    }
    
    @Override
    public void update(Double next) {
      if (min == null || min > next) {
        min = next;
      }
    }
    
    @Override
    public Iterable<Double> results() {
      return ImmutableList.of(min);
    }
  };

  public static class MaxNAggregator<V extends Comparable<V>> implements Aggregator<V> {
    private final int arity;
    private transient SortedSet<V> elements;

    public MaxNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = Sets.newTreeSet();
      } else {
        elements.clear();
      }
    }
    
    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (value.compareTo(elements.first()) > 0) {
        elements.remove(elements.first());
        elements.add(value);
      }
    }
    
    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }
  
  public static class MinNAggregator<V extends Comparable<V>> implements Aggregator<V> {
    private final int arity;
    private transient SortedSet<V> elements;
    
    public MinNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = Sets.newTreeSet();
      } else {
        elements.clear();
      }
    }
    
    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (value.compareTo(elements.last()) < 0) {
        elements.remove(elements.last());
        elements.add(value);
      }
    }
    
    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }
  
  public static class FirstNAggregator<V> implements Aggregator<V> {
    private final int arity;
    private final List<V> elements;
    
    public FirstNAggregator(int arity) {
      this.arity = arity;
      this.elements = Lists.newArrayList();
    }

    @Override
    public void reset() {
      elements.clear();
    }
    
    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      }
    }
    
    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

  public static class LastNAggregator<V> implements Aggregator<V> {
    private final int arity;
    private final LinkedList<V> elements;
    
    public LastNAggregator(int arity) {
      this.arity = arity;
      this.elements = Lists.newLinkedList();
    }

    @Override
    public void reset() {
      elements.clear();
    }
    
    @Override
    public void update(V value) {
      elements.add(value);
      if (elements.size() == arity + 1) {
        elements.removeFirst();
      }
    }
    
    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

}
