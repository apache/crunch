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

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.List;

import org.junit.Test;

import static com.cloudera.crunch.CombineFn.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CombineFnTest {

  private <T> Iterable<T> applyAggregator(AggregatorFactory<T> a, Iterable<T> values) {
    return applyAggregator(a.create(), values);
  }
  
  private <T> Iterable<T> applyAggregator(Aggregator<T> a, Iterable<T> values) {
    a.reset();
    for (T value : values) {
      a.update(value);
    }
    return a.results();
  }
  
  @Test
  public void testSums() {
    assertEquals(ImmutableList.of(1775L),
        applyAggregator(SUM_LONGS, ImmutableList.of(29L, 17L, 1729L)));

    assertEquals(ImmutableList.of(1765L),
        applyAggregator(SUM_LONGS, ImmutableList.of(29L, 7L, 1729L)));

    assertEquals(ImmutableList.of(1775),
        applyAggregator(SUM_INTS, ImmutableList.of(29, 17, 1729)));

    assertEquals(ImmutableList.of(1775.0f),
        applyAggregator(SUM_FLOATS, ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(ImmutableList.of(1775.0),
        applyAggregator(SUM_DOUBLES, ImmutableList.of(29.0, 17.0, 1729.0)));
    
    assertEquals(ImmutableList.of(new BigInteger("1775")),
        applyAggregator(SUM_BIGINTS,
            ImmutableList.of(new BigInteger("29"), new BigInteger("17"), new BigInteger("1729"))));
  }
  
  @Test
  public void testMax() {
    assertEquals(ImmutableList.of(1729L),
        applyAggregator(MAX_LONGS, ImmutableList.of(29L, 17L, 1729L)));
    
    assertEquals(ImmutableList.of(1729),
        applyAggregator(MAX_INTS, ImmutableList.of(29, 17, 1729)));

    assertEquals(ImmutableList.of(1729.0f),
        applyAggregator(MAX_FLOATS, ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(ImmutableList.of(1729.0),
        applyAggregator(MAX_DOUBLES, ImmutableList.of(29.0, 17.0, 1729.0)));
    
    assertEquals(ImmutableList.of(1745.0f),
        applyAggregator(MAX_FLOATS, ImmutableList.of(29f, 1745f, 17f, 1729f)));

    assertEquals(ImmutableList.of(new BigInteger("1729")),
        applyAggregator(MAX_BIGINTS,
            ImmutableList.of(new BigInteger("29"), new BigInteger("17"), new BigInteger("1729"))));
  }
  
  @Test
  public void testMin() {
    assertEquals(ImmutableList.of(17L),
        applyAggregator(MIN_LONGS, ImmutableList.of(29L, 17L, 1729L)));
    
    assertEquals(ImmutableList.of(17),
        applyAggregator(MIN_INTS, ImmutableList.of(29, 17, 1729)));

    assertEquals(ImmutableList.of(17.0f),
        applyAggregator(MIN_FLOATS, ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(ImmutableList.of(17.0),
        applyAggregator(MIN_DOUBLES, ImmutableList.of(29.0, 17.0, 1729.0)));
    
    assertEquals(ImmutableList.of(29),
        applyAggregator(MIN_INTS, ImmutableList.of(29, 170, 1729)));
    
    assertEquals(ImmutableList.of(new BigInteger("17")),
        applyAggregator(MIN_BIGINTS,
            ImmutableList.of(new BigInteger("29"), new BigInteger("17"), new BigInteger("1729"))));
  }

  @Test
  public void testMaxN() {
    assertEquals(ImmutableList.of(98, 1009), applyAggregator(new MaxNAggregator<Integer>(2),
        ImmutableList.of(17, 34, 98, 29, 1009)));
  }

  @Test
  public void testMinN() {
    assertEquals(ImmutableList.of(17, 29), applyAggregator(new MinNAggregator<Integer>(2),
        ImmutableList.of(17, 34, 98, 29, 1009)));
  }

  @Test
  public void testFirstN() {
    assertEquals(ImmutableList.of(17, 34), applyAggregator(new FirstNAggregator<Integer>(2),
        ImmutableList.of(17, 34, 98, 29, 1009)));
  }

  @Test
  public void testLastN() {
    assertEquals(ImmutableList.of(29, 1009), applyAggregator(new LastNAggregator<Integer>(2),
        ImmutableList.of(17, 34, 98, 29, 1009)));
  }
  
  @Test
  public void testPairs() {
    List<Pair<Long, Double>> input = ImmutableList.of(Pair.of(1720L, 17.29), Pair.of(9L, -3.14));
    Aggregator<Pair<Long, Double>> a = new PairAggregator<Long, Double>(
        SUM_LONGS.create(), MIN_DOUBLES.create());
    assertEquals(Pair.of(1729L, -3.14), Iterables.getOnlyElement(applyAggregator(a, input)));
  }
  
  @Test
  public void testPairsTwoLongs() {
    List<Pair<Long, Long>> input = ImmutableList.of(Pair.of(1720L, 1L), Pair.of(9L, 19L));
    Aggregator<Pair<Long, Long>> a = new PairAggregator<Long, Long>(
        SUM_LONGS.create(), SUM_LONGS.create());
    assertEquals(Pair.of(1729L, 20L), Iterables.getOnlyElement(applyAggregator(a, input)));
  }
  
  @Test
  public void testTrips() {
    List<Tuple3<Float, Double, Double>> input = ImmutableList.of(
        Tuple3.of(17.29f, 12.2, 0.1), Tuple3.of(3.0f, 1.2, 3.14), Tuple3.of(-1.0f, 14.5, -0.98));
    Aggregator<Tuple3<Float, Double, Double>> a = new TripAggregator<Float, Double, Double>(
        MAX_FLOATS.create(), MAX_DOUBLES.create(), MIN_DOUBLES.create());
    assertEquals(Tuple3.of(17.29f, 14.5, -0.98),
        Iterables.getOnlyElement(applyAggregator(a, input)));
  }
  
  @Test
  public void testQuads() {
    List<Tuple4<Float, Double, Double, Integer>> input = ImmutableList.of(
        Tuple4.of(17.29f, 12.2, 0.1, 1), Tuple4.of(3.0f, 1.2, 3.14, 2),
        Tuple4.of(-1.0f, 14.5, -0.98, 3));
    Aggregator<Tuple4<Float, Double, Double, Integer>> a =
        new QuadAggregator<Float, Double, Double, Integer>(MAX_FLOATS.create(),
            MAX_DOUBLES.create(), MIN_DOUBLES.create(), SUM_INTS.create());
    assertEquals(Tuple4.of(17.29f, 14.5, -0.98, 6),
        Iterables.getOnlyElement(applyAggregator(a, input)));
  }

  @Test
  public void testTupleN() {
    List<TupleN> input = ImmutableList.of(new TupleN(1, 3.0, 1, 2.0, 4L),
        new TupleN(4, 17.0, 1, 9.7, 12L));
    Aggregator<TupleN> a = new TupleNAggregator(MIN_INTS.create(), SUM_DOUBLES.create(),
        MAX_INTS.create(), MIN_DOUBLES.create(), MAX_LONGS.create());
    assertEquals(new TupleN(1, 20.0, 1, 2.0, 12L),
        Iterables.getOnlyElement(applyAggregator(a, input)));
  }
}
