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
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;

/**
 * Utilities for performing a secondary sort on a PTable<K, Pair<V1, V2>> instance, i.e., sort on the
 * key and then sort the values by V1.
 */
public class SecondarySort {

  public static <K, V1, V2> PTable<K, Collection<Pair<V1, V2>>> sort(PTable<K, Pair<V1, V2>> input) {
    PTypeFamily ptf = input.getTypeFamily();
    return sortAndApply(input, new SSUnpackFn<K, V1, V2>(),
        ptf.tableOf(input.getKeyType(), ptf.collections(input.getValueType())));
  }
  
  public static <K, V1, V2, T> PCollection<T> sortAndApply(PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, T> doFn, PType<T> ptype) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<V1, V2>> valueType = input.getValueType();
    PTableType<Pair<K, V1>, Pair<V1, V2>> inter = ptf.tableOf(
        ptf.pairs(input.getKeyType(), valueType.getSubTypes().get(0)),
        valueType);
    PTableType<K, Collection<Pair<V1, V2>>> out = ptf.tableOf(input.getKeyType(),
        ptf.collections(input.getValueType()));
    return input.parallelDo("SecondarySort.format", new SSFormatFn<K, V1, V2>(), inter)
        .groupByKey(
            GroupingOptions.builder()
            .groupingComparatorClass(JoinUtils.getGroupingComparator(ptf))
            .partitionerClass(JoinUtils.getPartitionerClass(ptf))
            .build())
        .parallelDo("SecondarySort.apply", new SSWrapFn<K, V1, V2, T>(doFn), ptype);
  }
  
  public static <K, V1, V2, U, V> PTable<U, V> sortAndApply(PTable<K, Pair<V1, V2>> input,
      DoFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<U, V>> doFn, PTableType<U, V> ptype) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<V1, V2>> valueType = input.getValueType();
    PTableType<Pair<K, V1>, Pair<V1, V2>> inter = ptf.tableOf(
        ptf.pairs(input.getKeyType(), valueType.getSubTypes().get(0)),
        valueType);
    PTableType<K, Collection<Pair<V1, V2>>> out = ptf.tableOf(input.getKeyType(),
        ptf.collections(input.getValueType()));
    return input.parallelDo("SecondarySort.format", new SSFormatFn<K, V1, V2>(), inter)
        .groupByKey(
            GroupingOptions.builder()
            .groupingComparatorClass(JoinUtils.getGroupingComparator(ptf))
            .partitionerClass(JoinUtils.getPartitionerClass(ptf))
            .build())
        .parallelDo("SecondarySort.apply", new SSTableWrapFn<K, V1, V2, U, V>(doFn), ptype);
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
    public void setConfigurationForTest(Configuration conf) {
      intern.setConfigurationForTest(conf);
    }

    @Override
    public void initialize() {
      intern.setContext(getContext());
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
  
  private static class SSTableWrapFn<K, V1, V2, U, V> extends DoFn<Pair<Pair<K, V1>, Iterable<Pair<V1, V2>>>, Pair<U, V>> {
    private final DoFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<U, V>> intern;
    
    public SSTableWrapFn(DoFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<U, V>> intern) {
      this.intern = intern;
    }

    @Override
    public void configure(Configuration conf) {
      intern.configure(conf);
    }

    @Override
    public void setConfigurationForTest(Configuration conf) {
      intern.setConfigurationForTest(conf);
    }

    @Override
    public void initialize() {
      intern.setContext(getContext());
    }
    
    @Override
    public void process(Pair<Pair<K, V1>, Iterable<Pair<V1, V2>>> input, Emitter<Pair<U, V>> emitter) {
      intern.process(Pair.of(input.first().first(), input.second()), emitter);
    }
    
    @Override
    public void cleanup(Emitter<Pair<U, V>> emitter) {
      intern.cleanup(emitter);
    }
  }
  
  private static class SSFormatFn<K, V1, V2> extends MapFn<Pair<K, Pair<V1, V2>>, Pair<Pair<K, V1>, Pair<V1, V2>>> {
    @Override
    public Pair<Pair<K, V1>, Pair<V1, V2>> map(Pair<K, Pair<V1, V2>> input) {
      return Pair.of(Pair.of(input.first(), input.second().first()), input.second());
    }
  }
  
  private static class SSUnpackFn<K, V1, V2> extends
      MapFn<Pair<K, Iterable<Pair<V1, V2>>>, Pair<K, Collection<Pair<V1, V2>>>> {
    @Override
    public Pair<K, Collection<Pair<V1, V2>>> map(Pair<K, Iterable<Pair<V1, V2>>> input) {
      Collection<Pair<V1, V2>> c = ImmutableList.copyOf(input.second());
      return Pair.of(input.first(), c);
    }
  }
}
