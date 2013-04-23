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

import static org.apache.crunch.fn.Aggregators.MAX_BIGINTS;
import static org.apache.crunch.fn.Aggregators.MAX_DOUBLES;
import static org.apache.crunch.fn.Aggregators.MAX_FLOATS;
import static org.apache.crunch.fn.Aggregators.MAX_INTS;
import static org.apache.crunch.fn.Aggregators.MAX_LONGS;
import static org.apache.crunch.fn.Aggregators.MAX_N;
import static org.apache.crunch.fn.Aggregators.MIN_BIGINTS;
import static org.apache.crunch.fn.Aggregators.MIN_DOUBLES;
import static org.apache.crunch.fn.Aggregators.MIN_FLOATS;
import static org.apache.crunch.fn.Aggregators.MIN_INTS;
import static org.apache.crunch.fn.Aggregators.MIN_LONGS;
import static org.apache.crunch.fn.Aggregators.MIN_N;
import static org.apache.crunch.fn.Aggregators.STRING_CONCAT;
import static org.apache.crunch.fn.Aggregators.SUM_BIGINTS;
import static org.apache.crunch.fn.Aggregators.SUM_DOUBLES;
import static org.apache.crunch.fn.Aggregators.SUM_FLOATS;
import static org.apache.crunch.fn.Aggregators.SUM_INTS;
import static org.apache.crunch.fn.Aggregators.SUM_LONGS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.Aggregator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


public class AggregatorsTest {

  @Test
  public void testSums2() {
    assertThat(sapply(SUM_INTS(), 1, 2, 3, -4), is(2));
    assertThat(sapply(SUM_LONGS(), 1L, 2L, 3L, -4L, 5000000000L), is(5000000002L));
    assertThat(sapply(SUM_FLOATS(), 1f, 2f, 3f, -4f), is(2f));
    assertThat(sapply(SUM_DOUBLES(), 0.1, 0.2, 0.3), is(closeTo(0.6, 0.00001)));
    assertThat(sapply(SUM_BIGINTS(), bigInt("7"), bigInt("3")), is(bigInt("10")));
  }

  @Test
  public void testSums() {
    assertThat(sapply(SUM_LONGS(), 29L, 17L, 1729L), is(1775L));
    assertThat(sapply(SUM_LONGS(), 29L, 7L, 1729L), is(1765L));
    assertThat(sapply(SUM_INTS(), 29, 17, 1729), is(1775));
    assertThat(sapply(SUM_FLOATS(), 29f, 17f, 1729f), is(1775.0f));
    assertThat(sapply(SUM_DOUBLES(), 29.0, 17.0, 1729.0), is(1775.0));
    assertThat(sapply(SUM_BIGINTS(), bigInt("29"), bigInt("17"), bigInt("1729")), is(bigInt("1775")));
  }

  @Test
  public void testMax() {
    assertThat(sapply(MAX_LONGS(), 29L, 17L, 1729L), is(1729L));
    assertThat(sapply(MAX_INTS(), 29, 17, 1729), is(1729));
    assertThat(sapply(MAX_FLOATS(), 29f, 17f, 1729f), is(1729.0f));
    assertThat(sapply(MAX_DOUBLES(), 29.0, 17.0, 1729.0), is(1729.0));
    assertThat(sapply(MAX_FLOATS(), 29f, 1745f, 17f, 1729f), is(1745.0f));
    assertThat(sapply(MAX_BIGINTS(), bigInt("29"), bigInt("17"), bigInt("1729")), is(bigInt("1729")));
  }

  @Test
  public void testMin() {
    assertThat(sapply(MIN_LONGS(), 29L, 17L, 1729L), is(17L));
    assertThat(sapply(MIN_INTS(), 29, 17, 1729), is(17));
    assertThat(sapply(MIN_FLOATS(), 29f, 17f, 1729f), is(17.0f));
    assertThat(sapply(MIN_DOUBLES(), 29.0, 17.0, 1729.0), is(17.0));
    assertThat(sapply(MIN_INTS(), 29, 170, 1729), is(29));
    assertThat(sapply(MIN_BIGINTS(), bigInt("29"), bigInt("17"), bigInt("1729")), is(bigInt("17")));
  }

  @Test
  public void testMaxN() {
    assertThat(apply(MAX_INTS(2), 17, 34, 98, 29, 1009), is(ImmutableList.of(98, 1009)));
    assertThat(apply(MAX_N(1, String.class), "b", "a"), is(ImmutableList.of("b")));
    assertThat(apply(MAX_N(3, String.class), "b", "a", "d", "c"), is(ImmutableList.of("b", "c", "d")));
  }

  @Test
  public void testMinN() {
    assertThat(apply(MIN_INTS(2), 17, 34, 98, 29, 1009), is(ImmutableList.of(17, 29)));
    assertThat(apply(MIN_N(1, String.class), "b", "a"), is(ImmutableList.of("a")));
    assertThat(apply(MIN_N(3, String.class), "b", "a", "d", "c"), is(ImmutableList.of("a", "b", "c")));
  }

  @Test
  public void testFirstN() {
    assertThat(apply(Aggregators.<Integer>FIRST_N(2), 17, 34, 98, 29, 1009), is(ImmutableList.of(17, 34)));
  }

  @Test
  public void testLastN() {
    assertThat(apply(Aggregators.<Integer>LAST_N(2), 17, 34, 98, 29, 1009), is(ImmutableList.of(29, 1009)));
  }
  
  @Test
  public void testUniqueElements() {
    assertThat(ImmutableSet.copyOf(apply(Aggregators.<Integer>UNIQUE_ELEMENTS(), 17, 29, 29, 16, 17)),
        is(ImmutableSet.of(17, 29, 16)));
    
    Iterable<Integer> samp = apply(Aggregators.<Integer>SAMPLE_UNIQUE_ELEMENTS(2), 17, 29, 16, 17, 29, 16);
    assertThat(Iterables.size(samp), is(2));
    assertThat(ImmutableSet.copyOf(samp).size(), is(2)); // check that the two elements are unique
  }
  
  @Test
  public void testPairs() {
    List<Pair<Long, Double>> input = ImmutableList.of(Pair.of(1720L, 17.29), Pair.of(9L, -3.14));
    Aggregator<Pair<Long, Double>> a = Aggregators.pairAggregator(SUM_LONGS(), MIN_DOUBLES());

    assertThat(sapply(a, input), is(Pair.of(1729L, -3.14)));
  }

  @Test
  public void testPairsTwoLongs() {
    List<Pair<Long, Long>> input = ImmutableList.of(Pair.of(1720L, 1L), Pair.of(9L, 19L));
    Aggregator<Pair<Long, Long>> a = Aggregators.pairAggregator(SUM_LONGS(), SUM_LONGS());

    assertThat(sapply(a, input), is(Pair.of(1729L, 20L)));
  }

  @Test
  public void testTrips() {
    List<Tuple3<Float, Double, Double>> input = ImmutableList.of(Tuple3.of(17.29f, 12.2, 0.1),
        Tuple3.of(3.0f, 1.2, 3.14), Tuple3.of(-1.0f, 14.5, -0.98));
    Aggregator<Tuple3<Float, Double, Double>> a = Aggregators.tripAggregator(
        MAX_FLOATS(), MAX_DOUBLES(), MIN_DOUBLES());

    assertThat(sapply(a, input), is(Tuple3.of(17.29f, 14.5, -0.98)));
  }

  @Test
  public void testQuads() {
    List<Tuple4<Float, Double, Double, Integer>> input = ImmutableList.of(Tuple4.of(17.29f, 12.2, 0.1, 1),
        Tuple4.of(3.0f, 1.2, 3.14, 2), Tuple4.of(-1.0f, 14.5, -0.98, 3));
    Aggregator<Tuple4<Float, Double, Double, Integer>> a = Aggregators.quadAggregator(
        MAX_FLOATS(), MAX_DOUBLES(), MIN_DOUBLES(), SUM_INTS());

    assertThat(sapply(a, input), is(Tuple4.of(17.29f, 14.5, -0.98, 6)));
  }

  @Test
  public void testTupleN() {
    List<TupleN> input = ImmutableList.of(new TupleN(1, 3.0, 1, 2.0, 4L), new TupleN(4, 17.0, 1, 9.7, 12L));
    Aggregator<TupleN> a = Aggregators.tupleAggregator(
        MIN_INTS(), SUM_DOUBLES(), MAX_INTS(), MIN_DOUBLES(), MAX_LONGS());

    assertThat(sapply(a, input), is(new TupleN(1, 20.0, 1, 2.0, 12L)));
  }

  @Test
  public void testConcatenation() {
    assertThat(sapply(STRING_CONCAT("", true), "foo", "foobar", "bar"), is("foofoobarbar"));
    assertThat(sapply(STRING_CONCAT("/", false), "foo", "foobar", "bar"), is("foo/foobar/bar"));
    assertThat(sapply(STRING_CONCAT(" ", true), " ", ""), is("  "));
    assertThat(sapply(STRING_CONCAT(" ", true), Arrays.asList(null, "")), is(""));
    assertThat(sapply(STRING_CONCAT(" ", true, 20, 3), "foo", "foobar", "bar"), is("foo bar"));
    assertThat(sapply(STRING_CONCAT(" ", true, 10, 6), "foo", "foobar", "bar"), is("foo foobar"));
    assertThat(sapply(STRING_CONCAT(" ", true, 9, 6), "foo", "foobar", "bar"), is("foo bar"));
  }

  @Test(expected = NullPointerException.class)
  public void testConcatenationNullException() {
    sapply(STRING_CONCAT(" ", false), Arrays.asList(null, "" ));
  }


  private static <T> T sapply(Aggregator<T> a, T... values) {
    return sapply(a, ImmutableList.copyOf(values));
  }

  private static <T> T sapply(Aggregator<T> a, Iterable<T> values) {
    return Iterables.getOnlyElement(apply(a, values));
  }

  private static <T> ImmutableList<T> apply(Aggregator<T> a, T... values) {
    return apply(a, ImmutableList.copyOf(values));
  }

  private static <T> ImmutableList<T> apply(Aggregator<T> a, Iterable<T> values) {
    CombineFn<String, T> fn = Aggregators.toCombineFn(a);

    InMemoryEmitter<Pair<String, T>> e1 = new InMemoryEmitter<Pair<String,T>>();
    fn.process(Pair.of("", values), e1);

    // and a second time to make sure Aggregator.reset() works
    InMemoryEmitter<Pair<String, T>> e2 = new InMemoryEmitter<Pair<String,T>>();
    fn.process(Pair.of("", values), e2);

    assertEquals(getValues(e1), getValues(e2));

    return getValues(e1);
  }

  private static <K, V> ImmutableList<V> getValues(InMemoryEmitter<Pair<K, V>> emitter) {
    return ImmutableList.copyOf(
        Iterables.transform(emitter.getOutput(), new Function<Pair<K, V>, V>() {
      @Override
      public V apply(Pair<K, V> input) {
        return input.second();
      }
    }));
  }

  private static BigInteger bigInt(String value) {
    return new BigInteger(value);
  }
}
