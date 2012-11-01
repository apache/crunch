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
package org.apache.crunch;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.util.Tuples;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A special {@link DoFn} implementation that converts an {@link Iterable} of
 * values into a single value. If a {@code CombineFn} instance is used on a
 * {@link PGroupedTable}, the function will be applied to the output of the map
 * stage before the data is passed to the reducer, which can improve the runtime
 * of certain classes of jobs.
 * 
 */
public abstract class CombineFn<S, T> extends DoFn<Pair<S, Iterable<T>>, Pair<S, T>> {

  /**
   * @deprecated Use {@link org.apache.crunch.Aggregator}
   */
  public static interface Aggregator<T> extends Serializable {
    /**
     * Perform any setup of this instance that is required prior to processing
     * inputs.
     */
    void initialize(Configuration configuration);

    /**
     * Clears the internal state of this Aggregator and prepares it for the
     * values associated with the next key.
     */
    void reset();

    /**
     * Incorporate the given value into the aggregate state maintained by this
     * instance.
     */
    void update(T value);

    /**
     * Returns the current aggregated state of this instance.
     */
    Iterable<T> results();
  }

  /**
   * Base class for aggregators that do not require any initialization.
   *
   * @deprecated Use {@link org.apache.crunch.fn.Aggregators.SimpleAggregator}
   */
  public static abstract class SimpleAggregator<T> implements Aggregator<T> {
    @Override
    public void initialize(Configuration conf) {
      // No-op
    }
  }
  
  /**
   * Interface for constructing new aggregator instances.
   *
   * @deprecated Use {@link PGroupedTable#combineValues(Aggregator)} which doesn't require a factory.
   */
  public static interface AggregatorFactory<T> {
    Aggregator<T> create();
  }

  /**
   * A {@code CombineFn} that delegates all of the actual work to an
   * {@code Aggregator} instance.
   *
   * @deprecated Use the {@link Aggregators#toCombineFn(org.apache.crunch.Aggregator)} adapter
   */
  public static class AggregatorCombineFn<K, V> extends CombineFn<K, V> {

    private final Aggregator<V> aggregator;

    public AggregatorCombineFn(Aggregator<V> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void initialize() {
      aggregator.initialize(getConfiguration());
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

    public TupleAggregator(Aggregator<?>... aggregators) {
      this.aggregators = Lists.newArrayList();
      for (Aggregator<?> a : aggregators) {
        this.aggregators.add((Aggregator<Object>) a);
      }
    }

    @Override
    public void initialize(Configuration configuration) {
      for (Aggregator<?> a : aggregators) {
        a.initialize(configuration);
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

  /**
   * @deprecated Use {@link Aggregators#pairAggregator(Aggregator, Aggregator)}
   */
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

  /**
   * @deprecated Use {@link Aggregators#tripAggregator(Aggregator, Aggregator, Aggregator)}
   */
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
      return new Tuples.TripIterable<A, B, C>((Iterable<A>) results(0), (Iterable<B>) results(1),
          (Iterable<C>) results(2));
    }
  }

  /**
   * @deprecated Use {@link Aggregators#quadAggregator(Aggregator, Aggregator, Aggregator, Aggregator)}
   */
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
      return new Tuples.QuadIterable<A, B, C, D>((Iterable<A>) results(0), (Iterable<B>) results(1),
          (Iterable<C>) results(2), (Iterable<D>) results(3));
    }
  }

  /**
   * @deprecated Use {@link Aggregators#tupleAggregator(Aggregator...)}
   */
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
      Iterable<?>[] iterables = new Iterable[size];
      for (int i = 0; i < size; i++) {
        iterables[i] = results(i);
      }
      return new Tuples.TupleNIterable(iterables);
    }

  }

  /**
   * @deprecated Use {@link Aggregators#toCombineFn(Aggregator)}
   */
  public static final <K, V> CombineFn<K, V> aggregator(Aggregator<V> aggregator) {
    return new AggregatorCombineFn<K, V>(aggregator);
  }

  /**
   * @deprecated Use {@link PGroupedTable#combineValues(Aggregator)} which doesn't require a factory.
   */
  public static final <K, V> CombineFn<K, V> aggregatorFactory(AggregatorFactory<V> aggregator) {
    return new AggregatorCombineFn<K, V>(aggregator.create());
  }

  /**
   * @deprecated Use {@link Aggregators#pairAggregator(Aggregator, Aggregator)}
   */
  public static final <K, V1, V2> CombineFn<K, Pair<V1, V2>> pairAggregator(AggregatorFactory<V1> a1,
      AggregatorFactory<V2> a2) {
    return aggregator(new PairAggregator<V1, V2>(a1.create(), a2.create()));
  }

  /**
   * @deprecated Use {@link Aggregators#tripAggregator(Aggregator, Aggregator, Aggregator)}
   */
  public static final <K, A, B, C> CombineFn<K, Tuple3<A, B, C>> tripAggregator(AggregatorFactory<A> a1,
      AggregatorFactory<B> a2, AggregatorFactory<C> a3) {
    return aggregator(new TripAggregator<A, B, C>(a1.create(), a2.create(), a3.create()));
  }

  /**
   * @deprecated Use {@link Aggregators#quadAggregator(Aggregator, Aggregator, Aggregator, Aggregator)}
   */
  public static final <K, A, B, C, D> CombineFn<K, Tuple4<A, B, C, D>> quadAggregator(AggregatorFactory<A> a1,
      AggregatorFactory<B> a2, AggregatorFactory<C> a3, AggregatorFactory<D> a4) {
    return aggregator(new QuadAggregator<A, B, C, D>(a1.create(), a2.create(), a3.create(), a4.create()));
  }

  /**
   * @deprecated Use {@link Aggregators#tupleAggregator(Aggregator...)}
   */
  public static final <K> CombineFn<K, TupleN> tupleAggregator(AggregatorFactory<?>... factories) {
    Aggregator<?>[] aggs = new Aggregator[factories.length];
    for (int i = 0; i < aggs.length; i++) {
      aggs[i] = factories[i].create();
    }
    return aggregator(new TupleNAggregator(aggs));
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_LONGS()}
   */
  public static final <K> CombineFn<K, Long> SUM_LONGS() {
    return aggregatorFactory(SUM_LONGS);
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_INTS()}
   */
  public static final <K> CombineFn<K, Integer> SUM_INTS() {
    return aggregatorFactory(SUM_INTS);
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_FLOATS()}
   */
  public static final <K> CombineFn<K, Float> SUM_FLOATS() {
    return aggregatorFactory(SUM_FLOATS);
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_DOUBLES()}
   */
  public static final <K> CombineFn<K, Double> SUM_DOUBLES() {
    return aggregatorFactory(SUM_DOUBLES);
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_BIGINTS()}
   */
  public static final <K> CombineFn<K, BigInteger> SUM_BIGINTS() {
    return aggregatorFactory(SUM_BIGINTS);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_LONGS()}
   */
  public static final <K> CombineFn<K, Long> MAX_LONGS() {
    return aggregatorFactory(MAX_LONGS);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_LONGS(int)}
   */
  public static final <K> CombineFn<K, Long> MAX_LONGS(int n) {
    return aggregator(new MaxNAggregator<Long>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_INTS()}
   */
  public static final <K> CombineFn<K, Integer> MAX_INTS() {
    return aggregatorFactory(MAX_INTS);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_INTS(int)}
   */
  public static final <K> CombineFn<K, Integer> MAX_INTS(int n) {
    return aggregator(new MaxNAggregator<Integer>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_FLOATS()}
   */
  public static final <K> CombineFn<K, Float> MAX_FLOATS() {
    return aggregatorFactory(MAX_FLOATS);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_FLOATS(int)}
   */
  public static final <K> CombineFn<K, Float> MAX_FLOATS(int n) {
    return aggregator(new MaxNAggregator<Float>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_DOUBLES()}
   */
  public static final <K> CombineFn<K, Double> MAX_DOUBLES() {
    return aggregatorFactory(MAX_DOUBLES);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_DOUBLES(int)}
   */
  public static final <K> CombineFn<K, Double> MAX_DOUBLES(int n) {
    return aggregator(new MaxNAggregator<Double>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_BIGINTS()}
   */
  public static final <K> CombineFn<K, BigInteger> MAX_BIGINTS() {
    return aggregatorFactory(MAX_BIGINTS);
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_BIGINTS(int)}
   */
  public static final <K> CombineFn<K, BigInteger> MAX_BIGINTS(int n) {
    return aggregator(new MaxNAggregator<BigInteger>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_LONGS()}
   */
  public static final <K> CombineFn<K, Long> MIN_LONGS() {
    return aggregatorFactory(MIN_LONGS);
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_LONGS(int)}
   */
  public static final <K> CombineFn<K, Long> MIN_LONGS(int n) {
    return aggregator(new MinNAggregator<Long>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_INTS()}
   */
  public static final <K> CombineFn<K, Integer> MIN_INTS() {
    return aggregatorFactory(MIN_INTS);
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_INTS(int)}
   */
  public static final <K> CombineFn<K, Integer> MIN_INTS(int n) {
    return aggregator(new MinNAggregator<Integer>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_FLOATS()}
   */
  public static final <K> CombineFn<K, Float> MIN_FLOATS() {
    return aggregatorFactory(MIN_FLOATS);
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_FLOATS(int)}
   */
  public static final <K> CombineFn<K, Float> MIN_FLOATS(int n) {
    return aggregator(new MinNAggregator<Float>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_DOUBLES()}
   */
  public static final <K> CombineFn<K, Double> MIN_DOUBLES() {
    return aggregatorFactory(MIN_DOUBLES);
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_DOUBLES(int)}
   */
  public static final <K> CombineFn<K, Double> MIN_DOUBLES(int n) {
    return aggregator(new MinNAggregator<Double>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_BIGINTS()}
   */
  public static final <K> CombineFn<K, BigInteger> MIN_BIGINTS() {
    return aggregatorFactory(MIN_BIGINTS);
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_BIGINTS(int)}
   */
  public static final <K> CombineFn<K, BigInteger> MIN_BIGINTS(int n) {
    return aggregator(new MinNAggregator<BigInteger>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#FIRST_N(int)}
   */
  public static final <K, V> CombineFn<K, V> FIRST_N(int n) {
    return aggregator(new FirstNAggregator<V>(n));
  }

  /**
   * @deprecated Use {@link Aggregators#LAST_N(int)}
   */
  public static final <K, V> CombineFn<K, V> LAST_N(int n) {
    return aggregator(new LastNAggregator<V>(n));
  }

  /**
   * Used to concatenate strings, with a separator between each strings. There
   * is no limits of length for the concatenated string.
   * 
   * @param separator
   *            the separator which will be appended between each string
   * @param skipNull
   *            define if we should skip null values. Throw
   *            NullPointerException if set to false and there is a null
   *            value.
   * @return
   *
   * @deprecated Use {@link Aggregators#STRING_CONCAT(String, boolean)}
   */
  public static final <K> CombineFn<K, String> STRING_CONCAT(final String separator, final boolean skipNull) {
      return aggregator(new StringConcatAggregator(separator, skipNull));
  }

  /**
   * Used to concatenate strings, with a separator between each strings. You
   * can specify the maximum length of the output string and of the input
   * strings, if they are > 0. If a value is <= 0, there is no limits.
   * 
   * Any too large string (or any string which would made the output too
   * large) will be silently discarded.
   * 
   * @param separator
   *            the separator which will be appended between each string
   * @param skipNull
   *            define if we should skip null values. Throw
   *            NullPointerException if set to false and there is a null
   *            value.
   * @param maxOutputLength
   *            the maximum length of the output string. If it's set <= 0,
   *            there is no limits. The number of characters of the output
   *            string will be < maxOutputLength.
   * @param maxInputLength
   *            the maximum length of the input strings. If it's set <= 0,
   *            there is no limits. The number of characters of the int string
   *            will be < maxInputLength to be concatenated.
   * @return
   *
   * @deprecated Use {@link Aggregators#STRING_CONCAT(String, boolean, long, long)}
   */
  public static final <K> CombineFn<K, String> STRING_CONCAT(final String separator, final boolean skipNull, final long maxOutputLength, final long maxInputLength) {
      return aggregator(new StringConcatAggregator(separator, skipNull, maxOutputLength, maxInputLength));
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_LONGS()}
   */
  public static class SumLongs extends SimpleAggregator<Long> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_LONGS()}
   */
  public static AggregatorFactory<Long> SUM_LONGS = new AggregatorFactory<Long>() {
    public Aggregator<Long> create() {
      return new SumLongs();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#SUM_INTS()}
   */
  public static class SumInts extends SimpleAggregator<Integer> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_INTS()}
   */
  public static AggregatorFactory<Integer> SUM_INTS = new AggregatorFactory<Integer>() {
    public Aggregator<Integer> create() {
      return new SumInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#SUM_FLOATS()}
   */
  public static class SumFloats extends SimpleAggregator<Float> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_FLOATS()}
   */
  public static AggregatorFactory<Float> SUM_FLOATS = new AggregatorFactory<Float>() {
    public Aggregator<Float> create() {
      return new SumFloats();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#SUM_DOUBLES()}
   */
  public static class SumDoubles extends SimpleAggregator<Double> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_DOUBLES()}
   */
  public static AggregatorFactory<Double> SUM_DOUBLES = new AggregatorFactory<Double>() {
    public Aggregator<Double> create() {
      return new SumDoubles();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#SUM_BIGINTS()}
   */
  public static class SumBigInts extends SimpleAggregator<BigInteger> {
    private BigInteger sum = BigInteger.ZERO;

    @Override
    public void reset() {
      sum = BigInteger.ZERO;
    }

    @Override
    public void update(BigInteger next) {
      sum = sum.add(next);
    }

    @Override
    public Iterable<BigInteger> results() {
      return ImmutableList.of(sum);
    }
  }

  /**
   * @deprecated Use {@link Aggregators#SUM_BIGINTS()}
   */
  public static AggregatorFactory<BigInteger> SUM_BIGINTS = new AggregatorFactory<BigInteger>() {
    public Aggregator<BigInteger> create() {
      return new SumBigInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_LONGS()}
   */
  public static class MaxLongs extends SimpleAggregator<Long> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_LONGS()}
   */
  public static AggregatorFactory<Long> MAX_LONGS = new AggregatorFactory<Long>() {
    public Aggregator<Long> create() {
      return new MaxLongs();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_INTS()}
   */
  public static class MaxInts extends SimpleAggregator<Integer> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_INTS()}
   */
  public static AggregatorFactory<Integer> MAX_INTS = new AggregatorFactory<Integer>() {
    public Aggregator<Integer> create() {
      return new MaxInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_FLOATS()}
   */
  public static class MaxFloats extends SimpleAggregator<Float> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_FLOATS()}
   */
  public static AggregatorFactory<Float> MAX_FLOATS = new AggregatorFactory<Float>() {
    public Aggregator<Float> create() {
      return new MaxFloats();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_DOUBLES()}
   */
  public static class MaxDoubles extends SimpleAggregator<Double> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_DOUBLES()}
   */
  public static AggregatorFactory<Double> MAX_DOUBLES = new AggregatorFactory<Double>() {
    public Aggregator<Double> create() {
      return new MaxDoubles();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_BIGINTS()}
   */
  public static class MaxBigInts extends SimpleAggregator<BigInteger> {
    private BigInteger max = null;

    @Override
    public void reset() {
      max = null;
    }

    @Override
    public void update(BigInteger next) {
      if (max == null || max.compareTo(next) < 0) {
        max = next;
      }
    }

    @Override
    public Iterable<BigInteger> results() {
      return ImmutableList.of(max);
    }
  }

  /**
   * @deprecated Use {@link Aggregators#MAX_BIGINTS()}
   */
  public static AggregatorFactory<BigInteger> MAX_BIGINTS = new AggregatorFactory<BigInteger>() {
    public Aggregator<BigInteger> create() {
      return new MaxBigInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MIN_LONGS()}
   */
  public static class MinLongs extends SimpleAggregator<Long> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_LONGS()}
   */
  public static AggregatorFactory<Long> MIN_LONGS = new AggregatorFactory<Long>() {
    public Aggregator<Long> create() {
      return new MinLongs();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MIN_INTS()}
   */
  public static class MinInts extends SimpleAggregator<Integer> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_INTS()}
   */
  public static AggregatorFactory<Integer> MIN_INTS = new AggregatorFactory<Integer>() {
    public Aggregator<Integer> create() {
      return new MinInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MIN_FLOATS()}
   */
  public static class MinFloats extends SimpleAggregator<Float> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_FLOATS()}
   */
  public static AggregatorFactory<Float> MIN_FLOATS = new AggregatorFactory<Float>() {
    public Aggregator<Float> create() {
      return new MinFloats();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MIN_DOUBLES()}
   */
  public static class MinDoubles extends SimpleAggregator<Double> {
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
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_DOUBLES()}
   */
  public static AggregatorFactory<Double> MIN_DOUBLES = new AggregatorFactory<Double>() {
    public Aggregator<Double> create() {
      return new MinDoubles();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MIN_BIGINTS()}
   */
  public static class MinBigInts extends SimpleAggregator<BigInteger> {
    private BigInteger min = null;

    @Override
    public void reset() {
      min = null;
    }

    @Override
    public void update(BigInteger next) {
      if (min == null || min.compareTo(next) > 0) {
        min = next;
      }
    }

    @Override
    public Iterable<BigInteger> results() {
      return ImmutableList.of(min);
    }
  }

  /**
   * @deprecated Use {@link Aggregators#MIN_BIGINTS()}
   */
  public static AggregatorFactory<BigInteger> MIN_BIGINTS = new AggregatorFactory<BigInteger>() {
    public Aggregator<BigInteger> create() {
      return new MinBigInts();
    }
  };

  /**
   * @deprecated Use {@link Aggregators#MAX_N(int, Class)}
   */
  public static class MaxNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
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

  /**
   * @deprecated Use {@link Aggregators#MIN_N(int, Class)}
   */
  public static class MinNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
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

  /**
   * @deprecated Use {@link Aggregators#FIRST_N(int)}
   */
  public static class FirstNAggregator<V> extends SimpleAggregator<V> {
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

  /**
   * @deprecated Use {@link Aggregators#LAST_N(int)}
   */
  public static class LastNAggregator<V> extends SimpleAggregator<V> {
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

  /**
   * @deprecated Use {@link Aggregators#STRING_CONCAT(String, boolean, long, long)}
   */
  public static class StringConcatAggregator extends SimpleAggregator<String> {
    private final String separator;
    private final boolean skipNulls;
    private final long maxOutputLength;
    private final long maxInputLength;
    private long currentLength;
    private final LinkedList<String> list = new LinkedList<String>();

    private transient Joiner joiner;
    
    public StringConcatAggregator(final String separator, final boolean skipNulls) {
      this.separator = separator;
      this.skipNulls = skipNulls;
      this.maxInputLength = 0;
      this.maxOutputLength = 0;
    }

    public StringConcatAggregator(final String separator, final boolean skipNull, final long maxOutputLength, final long maxInputLength) {
      this.separator = separator;
      this.skipNulls = skipNull;
      this.maxOutputLength = maxOutputLength;
      this.maxInputLength = maxInputLength;
      this.currentLength = -separator.length();
    }

    @Override
    public void reset() {
      if (joiner == null) {
        joiner = skipNulls ? Joiner.on(separator).skipNulls() : Joiner.on(separator);
      }
      currentLength = -separator.length();
      list.clear();
    }

    @Override
    public void update(final String next) {
      long length = (next == null) ? 0 : next.length() + separator.length();
      if (maxOutputLength > 0 && currentLength + length > maxOutputLength || maxInputLength > 0 && next.length() > maxInputLength) {
        return;
      }
      if (maxOutputLength > 0) {
        currentLength += length;
      }
      list.add(next);
    }

    @Override
    public Iterable<String> results() {
      return ImmutableList.of(joiner.join(list));
    }
  }
}
