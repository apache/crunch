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
package org.apache.crunch.fn;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.crunch.Aggregator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.Tuples;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * A collection of pre-defined {@link org.apache.crunch.Aggregator}s.
 *
 * <p>The factory methods of this class return {@link org.apache.crunch.Aggregator}
 * instances that you can use to combine the values of a {@link PGroupedTable}.
 * In most cases, they turn a multimap (multiple entries per key) into a map (one
 * entry per key).</p>
 *
 * <p><strong>Note</strong>: When using composed aggregators, like those built by the
 * {@link #pairAggregator(Aggregator, Aggregator) pairAggregator()}
 * factory method, you typically don't want to put in the same child aggregator more than once,
 * even if all child aggregators have the same type. In most cases, this is what you want:</p>
 *
 * <pre>
 *   PTable&lt;K, Long&gt; result = groupedTable.combineValues(
 *      pairAggregator(SUM_LONGS(), SUM_LONGS())
 *   );
 * </pre>
 */
public final class Aggregators {

  private Aggregators() {
    // utility class, not for instantiation
  }

  /**
   * Sum up all {@code long} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Long> SUM_LONGS() {
    return new SumLongs();
  }

  /**
   * Sum up all {@code int} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Integer> SUM_INTS() {
    return new SumInts();
  }

  /**
   * Sum up all {@code float} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Float> SUM_FLOATS() {
    return new SumFloats();
  }

  /**
   * Sum up all {@code double} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Double> SUM_DOUBLES() {
    return new SumDoubles();
  }

  /**
   * Sum up all {@link BigInteger} values.
   * @return The newly constructed instance
   */
  public static Aggregator<BigInteger> SUM_BIGINTS() {
    return new SumBigInts();
  }

  /**
   * Return the maximum of all given {@link Comparable} values.
   * @return The newly constructed instance
   */
  public static <C extends Comparable<C>> Aggregator<C> MAX_COMPARABLES() {
    return new MaxComparables<C>();
  }

  /**
   * Return the maximum of all given {@code long} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Long> MAX_LONGS() {
    return new MaxComparables<Long>();
  }

  /**
   * Return the {@code n} largest {@code long} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Long> MAX_LONGS(int n) {
    return new MaxNAggregator<Long>(n);
  }

  /**
   * Return the maximum of all given {@code int} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Integer> MAX_INTS() {
    return new MaxComparables<Integer>();
  }

  /**
   * Return the {@code n} largest {@code int} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Integer> MAX_INTS(int n) {
    return new MaxNAggregator<Integer>(n);
  }

  /**
   * Return the maximum of all given {@code float} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Float> MAX_FLOATS() {
    return new MaxComparables<Float>();
  }

  /**
   * Return the {@code n} largest {@code float} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Float> MAX_FLOATS(int n) {
    return new MaxNAggregator<Float>(n);
  }

  /**
   * Return the maximum of all given {@code double} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Double> MAX_DOUBLES() {
    return new MaxComparables<Double>();
  }

  /**
   * Return the {@code n} largest {@code double} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Double> MAX_DOUBLES(int n) {
    return new MaxNAggregator<Double>(n);
  }

  /**
   * Return the maximum of all given {@link BigInteger} values.
   * @return The newly constructed instance
   */
  public static Aggregator<BigInteger> MAX_BIGINTS() {
    return new MaxComparables<BigInteger>();
  }

  /**
   * Return the {@code n} largest {@link BigInteger} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<BigInteger> MAX_BIGINTS(int n) {
    return new MaxNAggregator<BigInteger>(n);
  }

  /**
   * Return the {@code n} largest values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @param cls The type of the values to aggregate (must implement {@link Comparable}!)
   * @return The newly constructed instance
   */
  public static <V extends Comparable<V>> Aggregator<V> MAX_N(int n, Class<V> cls) {
    return new MaxNAggregator<V>(n);
  }

  /**
   * Return the {@code n} largest unique values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @param cls The type of the values to aggregate (must implement {@link Comparable}!)
   * @return The newly constructed instance
   */
  public static <V extends Comparable<V>> Aggregator<V> MAX_UNIQUE_N(int n, Class<V> cls) {
    return new MaxUniqueNAggregator<V>(n);
  }

  /**
   * Return the minimum of all given {@link Comparable} values.
   * @return The newly constructed instance
   */
  public static <C extends Comparable<C>> Aggregator<C> MIN_COMPARABLES() {
    return new MinComparables<C>();
  }

  /**
   * Return the minimum of all given {@code long} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Long> MIN_LONGS() {
    return new MinComparables<Long>();
  }

  /**
   * Return the {@code n} smallest {@code long} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Long> MIN_LONGS(int n) {
    return new MinNAggregator<Long>(n);
  }

  /**
   * Return the minimum of all given {@code int} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Integer> MIN_INTS() {
    return new MinComparables<Integer>();
  }

  /**
   * Return the {@code n} smallest {@code int} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Integer> MIN_INTS(int n) {
    return new MinNAggregator<Integer>(n);
  }

  /**
   * Return the minimum of all given {@code float} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Float> MIN_FLOATS() {
    return new MinComparables<Float>();
  }

  /**
   * Return the {@code n} smallest {@code float} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Float> MIN_FLOATS(int n) {
    return new MinNAggregator<Float>(n);
  }

  /**
   * Return the minimum of all given {@code double} values.
   * @return The newly constructed instance
   */
  public static Aggregator<Double> MIN_DOUBLES() {
    return new MinComparables<Double>();
  }

  /**
   * Return the {@code n} smallest {@code double} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<Double> MIN_DOUBLES(int n) {
    return new MinNAggregator<Double>(n);
  }

  /**
   * Return the minimum of all given {@link BigInteger} values.
   * @return The newly constructed instance
   */
  public static Aggregator<BigInteger> MIN_BIGINTS() {
    return new MinComparables<BigInteger>();
  }

  /**
   * Return the {@code n} smallest {@link BigInteger} values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static Aggregator<BigInteger> MIN_BIGINTS(int n) {
    return new MinNAggregator<BigInteger>(n);
  }

  /**
   * Return the {@code n} smallest values (or fewer if there are fewer
   * values than {@code n}).
   * @param n The number of values to return
   * @param cls The type of the values to aggregate (must implement {@link Comparable}!)
   * @return The newly constructed instance
   */
  public static <V extends Comparable<V>> Aggregator<V> MIN_N(int n, Class<V> cls) {
    return new MinNAggregator<V>(n);
  }

  /**
   * Returns the {@code n} smallest unique values (or fewer if there are fewer unique values than {@code n}).
   * @param n The number of values to return
   * @param cls The type of the values to aggregate (must implement {@link Comparable}!)
   * @return The newly constructed instance
   */
  public static <V extends Comparable<V>> Aggregator<V> MIN_UNIQUE_N(int n, Class<V> cls) {
    return new MinUniqueNAggregator<V>(n);
  }

  /**
   * Return the first {@code n} values (or fewer if there are fewer values than {@code n}).
   *
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static <V> Aggregator<V> FIRST_N(int n) {
    return new FirstNAggregator<V>(n);
  }

  /**
   * Return the last {@code n} values (or fewer if there are fewer values than {@code n}).
   *
   * @param n The number of values to return
   * @return The newly constructed instance
   */
  public static <V> Aggregator<V> LAST_N(int n) {
    return new LastNAggregator<V>(n);
  }
  
  /**
   * Concatenate strings, with a separator between strings. There
   * is no limits of length for the concatenated string.
   *
   * <p><em>Note: String concatenation is not commutative, which means the
   * result of the aggregation is not deterministic!</em></p>
   *
   * @param separator
   *            the separator which will be appended between each string
   * @param skipNull
   *            define if we should skip null values. Throw
   *            NullPointerException if set to false and there is a null
   *            value.
   * @return The newly constructed instance
   */
  public static Aggregator<String> STRING_CONCAT(String separator, boolean skipNull) {
    return new StringConcatAggregator(separator, skipNull);
  }

  /**
   * Concatenate strings, with a separator between strings. You can specify
   * the maximum length of the output string and of the input strings, if
   * they are &gt; 0. If a value is &lt;= 0, there is no limit.
   *
   * <p>Any too large string (or any string which would made the output too
   * large) will be silently discarded.</p>
   *
   * <p><em>Note: String concatenation is not commutative, which means the
   * result of the aggregation is not deterministic!</em></p>
   *
   * @param separator
   *            the separator which will be appended between each string
   * @param skipNull
   *            define if we should skip null values. Throw
   *            NullPointerException if set to false and there is a null
   *            value.
   * @param maxOutputLength
   *            the maximum length of the output string. If it's set &lt;= 0,
   *            there is no limit. The number of characters of the output
   *            string will be &lt; maxOutputLength.
   * @param maxInputLength
   *            the maximum length of the input strings. If it's set <= 0,
   *            there is no limit. The number of characters of the input string
   *            will be &lt; maxInputLength to be concatenated.
   * @return The newly constructed instance
   */
  public static Aggregator<String> STRING_CONCAT(String separator, boolean skipNull,
      long maxOutputLength, long maxInputLength) {
    return new StringConcatAggregator(separator, skipNull, maxOutputLength, maxInputLength);
  }

  /**
   * Collect the unique elements of the input, as defined by the {@code equals} method for
   * the input objects. No guarantees are made about the order in which the final elements
   * will be returned.
   * 
   * @return The newly constructed instance
   */
  public static <V> Aggregator<V> UNIQUE_ELEMENTS() {
    return new SetAggregator<V>();
  }
  
  /**
   * Collect a sample of unique elements from the input, where 'unique' is defined by
   * the {@code equals} method for the input objects. No guarantees are made about which
   * elements will be returned, simply that there will not be any more than the given sample
   * size for any key.
   * 
   * @param maximumSampleSize The maximum number of unique elements to return per key
   * @return The newly constructed instance
   */
  public static <V> Aggregator<V> SAMPLE_UNIQUE_ELEMENTS(int maximumSampleSize) {
    return new SetAggregator<V>(maximumSampleSize);
  }
  
  /**
   * Apply separate aggregators to each component of a {@link Pair}.
   */
  public static <V1, V2> Aggregator<Pair<V1, V2>> pairAggregator(
      Aggregator<V1> a1, Aggregator<V2> a2) {
    return new PairAggregator<V1, V2>(a1, a2);
  }

  /**
   * Apply separate aggregators to each component of a {@link Tuple3}.
   */
  public static <V1, V2, V3> Aggregator<Tuple3<V1, V2, V3>> tripAggregator(
      Aggregator<V1> a1, Aggregator<V2> a2, Aggregator<V3> a3) {
    return new TripAggregator<V1, V2, V3>(a1, a2, a3);
  }

  /**
   * Apply separate aggregators to each component of a {@link Tuple4}.
   */
  public static <V1, V2, V3, V4> Aggregator<Tuple4<V1, V2, V3, V4>> quadAggregator(
      Aggregator<V1> a1, Aggregator<V2> a2, Aggregator<V3> a3, Aggregator<V4> a4) {
    return new QuadAggregator<V1, V2, V3, V4>(a1, a2, a3, a4);
  }

  /**
   * Apply separate aggregators to each component of a {@link Tuple}.
   */
  public static Aggregator<TupleN> tupleAggregator(Aggregator<?>... aggregators) {
    return new TupleNAggregator(aggregators);
  }

  /**
   * Wrap a {@link CombineFn} adapter around the given aggregator.
   *
   * @param aggregator The instance to wrap
   * @return A {@link CombineFn} delegating to {@code aggregator}
   *
   * @deprecated use the safer {@link #toCombineFn(Aggregator, PType)} instead.
   */
  @Deprecated
  public static final <K, V> CombineFn<K, V> toCombineFn(Aggregator<V> aggregator) {
    return toCombineFn(aggregator, null);
  }

  /**
   * Wrap a {@link CombineFn} adapter around the given aggregator.
   *
   * @param aggregator The instance to wrap
   * @param ptype The PType of the aggregated value (for detaching complex objects)
   * @return A {@link CombineFn} delegating to {@code aggregator}
   */
  public static final <K, V> CombineFn<K, V> toCombineFn(Aggregator<V> aggregator, PType<V> ptype) {
    return new AggregatorCombineFn<K, V>(aggregator, ptype);
  }

  /**
   * Base class for aggregators that do not require any initialization.
   */
  public static abstract class SimpleAggregator<T> implements Aggregator<T> {
    @Override
    public void initialize(Configuration conf) {
      // No-op
    }
  }

  /**
   * A {@code CombineFn} that delegates all of the actual work to an
   * {@code Aggregator} instance.
   */
  private static class AggregatorCombineFn<K, V> extends CombineFn<K, V> {
    // TODO: Has to be fully qualified until CombineFn.Aggregator can be removed.
    private final Aggregator<V> aggregator;
    private final PType<V> ptype;

    public AggregatorCombineFn(Aggregator<V> aggregator, PType<V> ptype) {
      this.aggregator = aggregator;
      this.ptype = ptype;
    }

    @Override
    public void initialize() {
      aggregator.initialize(getConfiguration());
      if (ptype != null) {
        ptype.initialize(getConfiguration());
      }
    }

    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      aggregator.reset();
      for (V v : input.second()) {
        aggregator.update(ptype == null ? v : ptype.getDetachedValue(v));
      }
      for (V v : aggregator.results()) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }
  }

  private static class SumLongs extends SimpleAggregator<Long> {
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

  private static class SumInts extends SimpleAggregator<Integer> {
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

  private static class SumFloats extends SimpleAggregator<Float> {
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

  private static class SumDoubles extends SimpleAggregator<Double> {
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

  private static class SumBigInts extends SimpleAggregator<BigInteger> {
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

  private static class MaxComparables<C extends Comparable<C>> extends SimpleAggregator<C> {

    private C max = null;

    @Override
    public void reset() {
      max = null;
    }

    @Override
    public void update(C next) {
      if (max == null || max.compareTo(next) < 0) {
        max = next;
      }
    }

    @Override
    public Iterable<C> results() {
      return ImmutableList.of(max);
    }
  }

  private static class MinComparables<C extends Comparable<C>> extends SimpleAggregator<C> {

    private C min = null;

    @Override
    public void reset() {
      min = null;
    }

    @Override
    public void update(C next) {
      if (min == null || min.compareTo(next) > 0) {
        min = next;
      }
    }

    @Override
    public Iterable<C> results() {
      return ImmutableList.of(min);
    }
  }

  private static class MaxNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
    private final int arity;
    private transient SortedMultiset<V> elements;

    public MaxNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = TreeMultiset.create();
      } else {
        elements.clear();
      }
    }

    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (value.compareTo(elements.firstEntry().getElement()) > 0) {
        elements.remove(elements.firstEntry().getElement());
        elements.add(value);
      }
    }

    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

  private static class MaxUniqueNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
    private final int arity;
    private transient SortedSet<V> elements;

    public MaxUniqueNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = new TreeSet<V>();
      } else {
        elements.clear();
      }
    }

    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (!elements.contains(value) && value.compareTo(elements.first()) > 0) {
        elements.remove(elements.first());
        elements.add(value);
      }
    }

    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

  private static class MinNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
    private final int arity;
    private transient SortedMultiset<V> elements;

    public MinNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = TreeMultiset.create();
      } else {
        elements.clear();
      }
    }

    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (value.compareTo(elements.lastEntry().getElement()) < 0) {
        elements.remove(elements.lastEntry().getElement());
        elements.add(value);
      }
    }

    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

  private static class MinUniqueNAggregator<V extends Comparable<V>> extends SimpleAggregator<V> {
    private final int arity;
    private transient SortedSet<V> elements;

    public MinUniqueNAggregator(int arity) {
      this.arity = arity;
    }

    @Override
    public void reset() {
      if (elements == null) {
        elements = new TreeSet<V>();
      } else {
        elements.clear();
      }
    }

    @Override
    public void update(V value) {
      if (elements.size() < arity) {
        elements.add(value);
      } else if (!elements.contains(value) && value.compareTo(elements.last()) < 0) {
        elements.remove(elements.last());
        elements.add(value);
      }
    }

    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }

  private static class FirstNAggregator<V> extends SimpleAggregator<V> {
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

  private static class LastNAggregator<V> extends SimpleAggregator<V> {
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

  private static class StringConcatAggregator extends SimpleAggregator<String> {
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
      if ((maxOutputLength > 0 && currentLength + length > maxOutputLength) ||
          (maxInputLength > 0 && next != null && next.length() > maxInputLength)) {
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


  private static abstract class TupleAggregator<T> implements Aggregator<T> {
    private final List<Aggregator<Object>> aggregators;

    @SuppressWarnings("unchecked")
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

  private static class PairAggregator<V1, V2> extends TupleAggregator<Pair<V1, V2>> {

    public PairAggregator(Aggregator<V1> a1, Aggregator<V2> a2) {
      super(a1, a2);
    }

    @Override
    public void update(Pair<V1, V2> value) {
      updateTuple(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Pair<V1, V2>> results() {
      return new Tuples.PairIterable<V1, V2>((Iterable<V1>) results(0), (Iterable<V2>) results(1));
    }
  }

  private static class TripAggregator<A, B, C> extends TupleAggregator<Tuple3<A, B, C>> {

    public TripAggregator(Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3) {
      super(a1, a2, a3);
    }

    @Override
    public void update(Tuple3<A, B, C> value) {
      updateTuple(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Tuple3<A, B, C>> results() {
      return new Tuples.TripIterable<A, B, C>((Iterable<A>) results(0), (Iterable<B>) results(1),
          (Iterable<C>) results(2));
    }
  }

  private static class QuadAggregator<A, B, C, D> extends TupleAggregator<Tuple4<A, B, C, D>> {

    public QuadAggregator(Aggregator<A> a1, Aggregator<B> a2, Aggregator<C> a3, Aggregator<D> a4) {
      super(a1, a2, a3, a4);
    }

    @Override
    public void update(Tuple4<A, B, C, D> value) {
      updateTuple(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Tuple4<A, B, C, D>> results() {
      return new Tuples.QuadIterable<A, B, C, D>((Iterable<A>) results(0), (Iterable<B>) results(1),
          (Iterable<C>) results(2), (Iterable<D>) results(3));
    }
  }

  private static class TupleNAggregator extends TupleAggregator<TupleN> {
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

  private static class SetAggregator<V> extends SimpleAggregator<V> {
    private final Set<V> elements;
    private final int sizeLimit;
    
    public SetAggregator() {
      this(-1);
    }
    
    public SetAggregator(int sizeLimit) {
      this.elements = Sets.newHashSet();
      this.sizeLimit = sizeLimit;
    }
    
    @Override
    public void reset() {
      elements.clear();
    }

    @Override
    public void update(V value) {
      if (sizeLimit == -1 || elements.size() < sizeLimit) {
        elements.add(value);
      }
    }

    @Override
    public Iterable<V> results() {
      return ImmutableList.copyOf(elements);
    }
  }
  
}
