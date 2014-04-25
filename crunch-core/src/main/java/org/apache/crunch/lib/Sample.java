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
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.SampleUtils.ReservoirSampleFn;
import org.apache.crunch.lib.SampleUtils.SampleFn;
import org.apache.crunch.lib.SampleUtils.WRSCombineFn;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Methods for performing random sampling in a distributed fashion, either by accepting each
 * record in a {@code PCollection} with an independent probability in order to sample some
 * fraction of the overall data set, or by using reservoir sampling in order to pull a uniform
 * or weighted sample of fixed size from a {@code PCollection} of an unknown size. For more details
 * on the reservoir sampling algorithms used by this library, see the A-ES algorithm described in
 * <a href="http://arxiv.org/pdf/1012.0256.pdf">Efraimidis (2012)</a>.
 */
public class Sample {

  /**
   * Output records from the given {@code PCollection} with the given probability.
   * 
   * @param input The {@code PCollection} to sample from
   * @param probability The probability (0.0 &lt; p %lt; 1.0)
   * @return The output {@code PCollection} created from sampling
   */
  public static <S> PCollection<S> sample(PCollection<S> input, double probability) {
    return sample(input, null, probability);
  }

  /**
   * Output records from the given {@code PCollection} using a given seed. Useful for unit
   * testing.
   * 
   * @param input The {@code PCollection} to sample from
   * @param seed The seed for the random number generator
   * @param probability The probability (0.0 &lt; p &lt; 1.0)
   * @return The output {@code PCollection} created from sampling
   */
  public static <S> PCollection<S> sample(PCollection<S> input, Long seed, double probability) {
    String stageName = String.format("sample(%.2f)", probability);
    return input.parallelDo(stageName, new SampleFn<S>(probability, seed), input.getPType());
  }
  
  /**
   * A {@code PTable<K, V>} analogue of the {@code sample} function.
   * 
   * @param input The {@code PTable} to sample from
   * @param probability The probability (0.0 &lt; p &lt; 1.0)
   * @return The output {@code PTable} created from sampling
   */
  public static <K, V> PTable<K, V> sample(PTable<K, V> input, double probability) {
    return PTables.asPTable(sample((PCollection<Pair<K, V>>) input, probability));
  }
  
  /**
   * A {@code PTable<K, V>} analogue of the {@code sample} function, with the seed argument
   * exposed for testing purposes.
   * 
   * @param input The {@code PTable} to sample from
   * @param seed The seed for the random number generator
   * @param probability The probability (0.0 &lt; p &lt; 1.0)
   * @return The output {@code PTable} created from sampling
   */
  public static <K, V> PTable<K, V> sample(PTable<K, V> input, Long seed, double probability) {
    return PTables.asPTable(sample((PCollection<Pair<K, V>>) input, seed, probability));
  }
  
  /**
   * Select a fixed number of elements from the given {@code PCollection} with each element
   * equally likely to be included in the sample.
   * 
   * @param input The input data
   * @param sampleSize The number of elements to select
   * @return A {@code PCollection} made up of the sampled elements
   */
  public static <T> PCollection<T> reservoirSample(
      PCollection<T> input,
      int sampleSize) {
    return reservoirSample(input, sampleSize, null);
  }

  /**
   * A version of the reservoir sampling algorithm that uses a given seed, primarily for
   * testing purposes.
   * 
   * @param input The input data
   * @param sampleSize The number of elements to select
   * @param seed The test seed
   * @return A {@code PCollection} made up of the sampled elements

   */
  public static <T> PCollection<T> reservoirSample(
      PCollection<T> input,
      int sampleSize,
      Long seed) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<T, Integer>> ptype = ptf.pairs(input.getPType(), ptf.ints());
    return weightedReservoirSample(
        input.parallelDo("Map to pairs for reservoir sampling", new MapFn<T, Pair<T, Integer>>() {
          @Override
          public Pair<T, Integer> map(T t) { return Pair.of(t, 1); }
        }, ptype),
        sampleSize,
        seed);
  }
  
  /**
   * Selects a weighted sample of the elements of the given {@code PCollection}, where the second term in
   * the input {@code Pair} is a numerical weight.
   * 
   * @param input the weighted observations
   * @param sampleSize The number of elements to select
   * @return A random sample of the given size that respects the weighting values
   */
  public static <T, N extends Number> PCollection<T> weightedReservoirSample(
      PCollection<Pair<T, N>> input,
      int sampleSize) {
    return weightedReservoirSample(input, sampleSize, null);
  }
  
  /**
   * The weighted reservoir sampling function with the seed term exposed for testing purposes.
   * 
   * @param input the weighted observations
   * @param sampleSize The number of elements to select
   * @param seed The test seed
   * @return A random sample of the given size that respects the weighting values
   */
  public static <T, N extends Number> PCollection<T> weightedReservoirSample(
      PCollection<Pair<T, N>> input,
      int sampleSize,
      Long seed) {
    PTypeFamily ptf = input.getTypeFamily();
    PTable<Integer, Pair<T, N>> groupedIn = input.parallelDo(
        new MapFn<Pair<T, N>, Pair<Integer, Pair<T, N>>>() {
          @Override
          public Pair<Integer, Pair<T, N>> map(Pair<T, N> p) {
            return Pair.of(0, p);
          }
        }, ptf.tableOf(ptf.ints(), input.getPType()));
    int[] ss = { sampleSize };
    return groupedWeightedReservoirSample(groupedIn, ss, seed)
        .parallelDo("Extract sampled value from pair", new MapFn<Pair<Integer, T>, T>() {
          @Override
          public T map(Pair<Integer, T> p) {
            return p.second();
          }
        }, (PType<T>) input.getPType().getSubTypes().get(0));
  }
  
  /**
   * The most general purpose of the weighted reservoir sampling patterns that allows us to choose
   * a random sample of elements for each of N input groups.
   * 
   * @param input A {@code PTable} with the key a group ID and the value a weighted observation in that group
   * @param sampleSizes An array of length N, with each entry is the number of elements to include in that group
   * @return A {@code PCollection} of the sampled elements for each of the groups
   */
  
  public static <T, N extends Number> PCollection<Pair<Integer, T>> groupedWeightedReservoirSample(
      PTable<Integer, Pair<T, N>> input,
      int[] sampleSizes) {
    return groupedWeightedReservoirSample(input, sampleSizes, null);
  }
  
  /**
   * Same as the other groupedWeightedReservoirSample method, but include a seed for testing
   * purposes.
   * 
   * @param input A {@code PTable} with the key a group ID and the value a weighted observation in that group
   * @param sampleSizes An array of length N, with each entry is the number of elements to include in that group
   * @param seed The test seed
   * @return A {@code PCollection} of the sampled elements for each of the groups
   */
  public static <T, N extends Number> PCollection<Pair<Integer, T>> groupedWeightedReservoirSample(
      PTable<Integer, Pair<T, N>> input,
      int[] sampleSizes,
      Long seed) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<T> ttype = (PType<T>) input.getPTableType().getValueType().getSubTypes().get(0);
    PTableType<Integer, Pair<Double, T>> ptt = ptf.tableOf(ptf.ints(),
        ptf.pairs(ptf.doubles(), ttype));
    
    return input.parallelDo("Initial reservoir sampling", new ReservoirSampleFn<T, N>(sampleSizes, seed, ttype), ptt)
        .groupByKey(1)
        .combineValues(new WRSCombineFn<T>(sampleSizes, ttype))
        .parallelDo("Extract sampled values", new MapFn<Pair<Integer, Pair<Double, T>>, Pair<Integer, T>>() {
          @Override
          public Pair<Integer, T> map(Pair<Integer, Pair<Double, T>> p) {
            return Pair.of(p.first(), p.second().second());
          }
        }, ptf.pairs(ptf.ints(), ttype));
  }

}
