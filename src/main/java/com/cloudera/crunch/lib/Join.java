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
package com.cloudera.crunch.lib;

import java.util.List;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.avro.AvroTypeFamily;
import com.google.common.collect.Lists;

/**
 * Utilites for joining multiple {@code PTable} instances based on a common
 * key.
 *
 */
public class Join {

  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right) {
    PTypeFamily ptf = left.getTypeFamily();
    PTableType<Pair<K, Integer>, Pair<U, V>> ptt = ptf.tableOf(ptf.pairs(left.getKeyType(), ptf.ints()),
        ptf.pairs(left.getValueType(), right.getValueType()));
    
    PTable<Pair<K, Integer>, Pair<U, V>> tag1 = left.parallelDo("tag:" + left.getName(),
        new MapFn<Pair<K, U>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, U> input) {
            return Pair.of(Pair.of(input.first(), 0), Pair.of(input.second(), (V) null));
          }
        }, ptt);
    PTable<Pair<K, Integer>, Pair<U, V>> tag2 = right.parallelDo("tag:" + right.getName(),
        new MapFn<Pair<K, V>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, V> input) {
            return Pair.of(Pair.of(input.first(), 1), Pair.of((U) null, input.second()));
          }
        }, ptt);
    
    PTable<Pair<K, Integer>, Pair<U, V>> both = tag1.union(tag2);
    
    GroupingOptions.Builder optionsBuilder = GroupingOptions.builder();
    if (ptf == AvroTypeFamily.getInstance()) {
      optionsBuilder.partitionerClass(JoinUtils.AvroPairPartitioner.class);
    } else {
      optionsBuilder.partitionerClass(JoinUtils.TupleWritablePartitioner.class);
    }
    
    PGroupedTable<Pair<K, Integer>, Pair<U, V>> grouped = both.groupByKey(
        optionsBuilder.build());
    PTableType<K, Pair<U, V>> ret = ptf.tableOf(left.getKeyType(),
        ptf.pairs(left.getValueType(), right.getValueType()));
    return grouped.parallelDo("untag:" + grouped.getName(),
        new DoFn<Pair<Pair<K, Integer>, Iterable<Pair<U, V>>>, Pair<K, Pair<U, V>>>() {  
          private transient K key;
          private transient List<U> firstValues;
          
          @Override
          public void initialize() {
            key = null;
            this.firstValues = Lists.newArrayList();
          }
          
          @Override
          public void process(Pair<Pair<K, Integer>, Iterable<Pair<U, V>>> input,
              Emitter<Pair<K, Pair<U, V>>> emitter) {
            if (!input.first().first().equals(key)) {
              key = input.first().first();
              firstValues.clear();
            }
            if (input.first().second() == 0) {
              for (Pair<U, V> pair : input.second()) {
                if (pair.first() != null)
                  firstValues.add(pair.first());
              }
            } else {
              for (Pair<U, V> pair : input.second()) {
                for (U u : firstValues) {
                  emitter.emit(Pair.of(key, Pair.of(u, pair.second())));
                }
              }
            }
          }
        }, ret);
  }
}
