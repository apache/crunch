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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;

/**
 * Utilities for performing a secondary sort on a {@code PTable<K, Pair<V1, V2>>} collection.
 * <p>
 * Secondary sorts are usually performed during sessionization: given a collection
 * of events, we want to group them by a key (such as a user ID), then sort the grouped
 * records by an auxillary key (such as a timestamp), and then perform some additional
 * processing on the sorted records.
 */
public class SecondarySort {
  
  /**
   * Perform a secondary sort on the given {@code PTable} instance and then apply a
   * {@code DoFn} to the resulting sorted data to yield an output {@code PCollection<T>}.
   */
  public static <K, V1, V2, T> PCollection<T> sortAndApply(
      PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, T> doFn,
      PType<T> ptype) {
    return sortAndApply(input, doFn, ptype, -1);
  }
  
  /**
   * Perform a secondary sort on the given {@code PTable} instance and then apply a
   * {@code DoFn} to the resulting sorted data to yield an output {@code PCollection<T>}, using
   * the given number of reducers.
   */
  public static <K, V1, V2, T> PCollection<T> sortAndApply(
      PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, T> doFn,
      PType<T> ptype,
      int numReducers) {
    return prepare(input, numReducers)
        .parallelDo("SecondarySort.apply", new SSWrapFn<K, V1, V2, T>(doFn), ptype);
  }
 
  /**
   * Perform a secondary sort on the given {@code PTable} instance and then apply a
   * {@code DoFn} to the resulting sorted data to yield an output {@code PTable<U, V>}.
   */
  public static <K, V1, V2, U, V> PTable<U, V> sortAndApply(
      PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<U, V>> doFn,
      PTableType<U, V> ptype) {
    return sortAndApply(input, doFn, ptype, -1);
  }
  
  /**
   * Perform a secondary sort on the given {@code PTable} instance and then apply a
   * {@code DoFn} to the resulting sorted data to yield an output {@code PTable<U, V>}, using
   * the given number of reducers.
   */
  public static <K, V1, V2, U, V> PTable<U, V> sortAndApply(
      PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<U, V>> doFn,
      PTableType<U, V> ptype,
      int numReducers) {
    return prepare(input, numReducers)
        .parallelDo("SecondarySort.apply", new SSWrapFn<K, V1, V2, Pair<U, V>>(doFn), ptype);
  }
  
  private static <K, V1, V2> PGroupedTable<Pair<K, V1>, Pair<V1, V2>> prepare(
      PTable<K, Pair<V1, V2>> input, int numReducers) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<V1, V2>> valueType = input.getValueType();
    PTableType<Pair<K, V1>, Pair<V1, V2>> inter = ptf.tableOf(
        ptf.pairs(input.getKeyType(), valueType.getSubTypes().get(0)),
        valueType);
    GroupingOptions.Builder gob = GroupingOptions.builder()
        .requireSortedKeys()
        .groupingComparatorClass(JoinUtils.getGroupingComparator(ptf))
        .partitionerClass(JoinUtils.getPartitionerClass(ptf));
    if (numReducers > 0) {
      gob.numReducers(numReducers);
    }
    return input.parallelDo("SecondarySort.format", new SSFormatFn<K, V1, V2>(), inter)
        .groupByKey(gob.build());
  }
  
  private static class SSFormatFn<K, V1, V2> extends MapFn<Pair<K, Pair<V1, V2>>, Pair<Pair<K, V1>, Pair<V1, V2>>> {
    @Override
    public Pair<Pair<K, V1>, Pair<V1, V2>> map(Pair<K, Pair<V1, V2>> input) {
      return Pair.of(Pair.of(input.first(), input.second().first()), input.second());
    }
  }  

  private static class SSWrapFn<K, V1, V2, T> extends DoFn<Pair<Pair<K, V1>, Iterable<Pair<V1, V2>>>, T> {
    private final DoFn<Pair<K, Iterable<Pair<V1, V2>>>, T> intern;
    
    public SSWrapFn(DoFn<Pair<K, Iterable<Pair<V1, V2>>>, T> intern) {
      this.intern = intern;
    }

    @Override
    public void configure(Configuration conf) {
      intern.configure(conf);
    }

    @Override
    public void initialize() {
      intern.setContext(getContext());
      intern.initialize();
    }
    
    @Override
    public void process(Pair<Pair<K, V1>, Iterable<Pair<V1, V2>>> input, Emitter<T> emitter) {
      intern.process(Pair.of(input.first().first(), input.second()), emitter);
    }
    
    @Override
    public void cleanup(Emitter<T> emitter) {
      intern.cleanup(emitter);
    }
  }  
}
