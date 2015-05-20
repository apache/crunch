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

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Quantiles {

  /**
   * Calculate a set of quantiles for each key in a numerically-valued table.
   *
   * Quantiles are calculated on a per-key basis by counting, joining and sorting. This is highly scalable, but takes
   * 2 more map-reduce cycles than if you can guarantee that the value set will fit into memory. Use inMemory
   * if you have less than the order of 10M values per key.
   *
   * The quantile definition that we use here is the "nearest rank" defined here:
   * http://en.wikipedia.org/wiki/Percentile#Definition
   *
   * @param table numerically-valued PTable
   * @param p1 First quantile (in the range 0.0 - 1.0)
   * @param pn More quantiles (in the range 0.0 - 1.0)
   * @param <K> Key type of the table
   * @param <V> Value type of the table (must extends java.lang.Number)
   * @return PTable of each key with a collection of pairs of the quantile provided and it's result.
   */
  public static <K, V extends Number> PTable<K, Result<V>> distributed(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> quantileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();
    PTable<K, Long> totalCounts = table.keys().count();
    PTable<K, Pair<Long, V>> countValuePairs = totalCounts.join(table);
    PTable<K, Pair<V, Long>> valueCountPairs =
            countValuePairs.mapValues(new SwapPairComponents<Long, V>(), ptf.pairs(table.getValueType(), ptf.longs()));


    return SecondarySort.sortAndApply(
            valueCountPairs,
            new DistributedQuantiles<K, V>(quantileList),
            ptf.tableOf(table.getKeyType(), Result.pType(table.getValueType())));
  }

  /**
   * Calculate a set of quantiles for each key in a numerically-valued table.
   *
   * Quantiles are calculated on a per-key basis by grouping, reading the data into memory, then sorting and
   * and calculating. This is much faster than the distributed option, but if you get into the order of 10M+ per key, then
   * performance might start to degrade or even cause OOMs.
   *
   * The quantile definition that we use here is the "nearest rank" defined here:
   * http://en.wikipedia.org/wiki/Percentile#Definition
   *
   * @param table numerically-valued PTable
   * @param p1 First quantile (in the range 0.0 - 1.0)
   * @param pn More quantiles (in the range 0.0 - 1.0)
   * @param <K> Key type of the table
   * @param <V> Value type of the table (must extends java.lang.Number)
   * @return PTable of each key with a collection of pairs of the quantile provided and it's result.
   */
  public static <K, V extends Comparable> PTable<K, Result<V>> inMemory(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> quantileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();

    return table
            .groupByKey()
            .parallelDo(new InMemoryQuantiles<K, V>(quantileList),
                        ptf.tableOf(table.getKeyType(), Result.pType(table.getValueType())));
  }

  private static List<Double> createListFromVarargs(double p1, double[] pn) {
    final List<Double> quantileList = Lists.newArrayList(p1);
    for (double p: pn) {
      quantileList.add(p);
    }
    return quantileList;
  }

  private static class SwapPairComponents<T1, T2> extends MapFn<Pair<T1, T2>, Pair<T2, T1>> {
    @Override
    public Pair<T2, T1> map(Pair<T1, T2> input) {
      return Pair.of(input.second(), input.first());
    }
  }

  private static <V> Collection<Pair<Double, V>> findQuantiles(Iterator<V> sortedCollectionIterator,
          long collectionSize, List<Double> quantiles) {
    Collection<Pair<Double, V>> output = Lists.newArrayList();
    Multimap<Long, Double> quantileIndices = ArrayListMultimap.create();

    for (double quantile: quantiles) {
      long idx = Math.max((int) Math.ceil(quantile * collectionSize) - 1, 0);
      quantileIndices.put(idx, quantile);
    }

    long index = 0;
    while (sortedCollectionIterator.hasNext()) {
      V value = sortedCollectionIterator.next();
      if (quantileIndices.containsKey(index)) {
        for (double quantile: quantileIndices.get(index)) {
          output.add(Pair.of(quantile, value));
        }
      }
      index++;
    }
    return output;
  }

  private static class InMemoryQuantiles<K, V extends Comparable> extends
          MapFn<Pair<K, Iterable<V>>, Pair<K, Result<V>>> {
    private final List<Double> quantileList;

    public InMemoryQuantiles(List<Double> quantiles) {
      this.quantileList = quantiles;
    }

    @Override
    public Pair<K, Result<V>> map(Pair<K, Iterable<V>> input) {
      List<V> values = Lists.newArrayList(input.second().iterator());
      Collections.sort(values);
      return Pair.of(input.first(), new Result<V>(values.size(), findQuantiles(values.iterator(), values.size(), quantileList)));
    }
  }

  private static class DistributedQuantiles<K, V> extends
          MapFn<Pair<K, Iterable<Pair<V, Long>>>, Pair<K, Result<V>>> {
    private final List<Double> quantileList;

    public DistributedQuantiles(List<Double> quantileList) {
      this.quantileList = quantileList;
    }

    @Override
    public Pair<K, Result<V>> map(Pair<K, Iterable<Pair<V, Long>>> input) {

      PeekingIterator<Pair<V, Long>> iterator = Iterators.peekingIterator(input.second().iterator());
      long count = iterator.peek().second();

      Iterator<V> valueIterator = Iterators.transform(iterator, new Function<Pair<V, Long>, V>() {
        @Override
        public V apply(Pair<V, Long> input) {
          return input.first();
        }
      });

      Collection<Pair<Double, V>> output = findQuantiles(valueIterator, count, quantileList);
      return Pair.of(input.first(), new Result<V>(count, output));
    }
  }

  /**
   * Output type for storing the results of a Quantiles computation
   * @param <V> Quantile value type
   */
  public static class Result<V> {
    public final long count;
    public final Map<Double, V> quantiles = Maps.newTreeMap();

    public Result(long count, Iterable<Pair<Double, V>> quantiles) {
      this.count = count;
      for (Pair<Double,V> quantile: quantiles) {
        this.quantiles.put(quantile.first(), quantile.second());
      }
    }

    /**
     * Create a PType for the result type, to be stored as a derived type from Crunch primitives
     * @param valuePType PType for the V type, whose family will also be used to create the derived type
     * @param <V> Value type
     * @return PType for serializing Result&lt;V&gt;
     */
    public static <V> PType<Result<V>> pType(PType<V> valuePType) {
      PTypeFamily ptf = valuePType.getFamily();

      @SuppressWarnings("unchecked")
      Class<Result<V>> prClass = (Class<Result<V>>)(Class)Result.class;

      return ptf.derivedImmutable(prClass, new MapFn<Pair<Collection<Pair<Double, V>>, Long>, Result<V>>() {
        @Override
        public Result<V> map(Pair<Collection<Pair<Double, V>>, Long> input) {
          return new Result<V>(input.second(), input.first());
        }
      }, new MapFn<Result<V>, Pair<Collection<Pair<Double, V>>, Long>>() {
        @Override
        public Pair<Collection<Pair<Double, V>>, Long> map(Result<V> input) {
          return Pair.of(asCollection(input.quantiles), input.count);
        }
      }, ptf.pairs(ptf.collections(ptf.pairs(ptf.doubles(), valuePType)), ptf.longs()));
    }

    private static <K, V> Collection<Pair<K, V>> asCollection(Map<K, V> map) {
      Collection<Pair<K, V>> collection = Lists.newArrayList();
      for (Map.Entry<K, V> entry: map.entrySet()) {
        collection.add(Pair.of(entry.getKey(), entry.getValue()));
      }
      return collection;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Result result = (Result) o;

      if (count != result.count) return false;
      if (!quantiles.equals(result.quantiles)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (count ^ (count >>> 32));
      result = 31 * result + quantiles.hashCode();
      return result;
    }
  }
}
