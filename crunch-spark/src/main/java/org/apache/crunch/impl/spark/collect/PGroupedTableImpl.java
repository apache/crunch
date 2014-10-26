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
package org.apache.crunch.impl.spark.collect;

import org.apache.crunch.CombineFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.impl.dist.collect.BaseGroupedTable;
import org.apache.crunch.impl.dist.collect.PTableBase;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkComparator;
import org.apache.crunch.impl.spark.SparkPartitioner;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.fn.CombineMapsideFunction;
import org.apache.crunch.impl.spark.fn.MapOutputFunction;
import org.apache.crunch.impl.spark.fn.PairMapFunction;
import org.apache.crunch.impl.spark.fn.PairMapIterableFunction;
import org.apache.crunch.impl.spark.fn.PartitionedMapOutputFunction;
import org.apache.crunch.impl.spark.fn.ReduceGroupingFunction;
import org.apache.crunch.impl.spark.fn.ReduceInputFunction;
import org.apache.crunch.impl.spark.serde.AvroSerDe;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.crunch.impl.spark.serde.WritableSerDe;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.util.PartitionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PGroupedTableImpl<K, V> extends BaseGroupedTable<K, V> implements SparkCollection {

  private static final Logger LOG = LoggerFactory.getLogger(PGroupedTableImpl.class);

  private JavaRDDLike<?, ?> rdd;

  PGroupedTableImpl(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    super(parent, groupingOptions);
  }

  public JavaRDDLike<?, ?> getJavaRDDLike(SparkRuntime runtime) {
    if (!runtime.isValid(rdd)) {
      rdd = getJavaRDDLikeInternal(runtime, runtime.getCombineFn());
      rdd.rdd().setName(getName());
      StorageLevel sl = runtime.getStorageLevel(this);
      if (sl != null) {
        rdd.rdd().persist(sl);
      }
    }
    return rdd;
  }

  private AvroSerDe getAvroSerde(PType ptype, Configuration conf) {
    AvroType at = (AvroType) ptype;
    Map<String, String> props = AvroMode.fromType(at).withFactoryFromConfiguration(conf).getModeProperties();
    return new AvroSerDe(at, props);
  }

  private JavaRDDLike<?, ?> getJavaRDDLikeInternal(SparkRuntime runtime, CombineFn<K, V> combineFn) {
    JavaPairRDD<K, V> parentRDD = (JavaPairRDD<K, V>) ((SparkCollection)getOnlyParent()).getJavaRDDLike(runtime);
    if (combineFn != null) {
      parentRDD = parentRDD.mapPartitionsToPair(
          new CombineMapsideFunction<K, V>(combineFn, runtime.getRuntimeContext()));
    }
    SerDe keySerde, valueSerde;
    PTableType<K, V> parentType = ptype.getTableType();
    if (parentType instanceof AvroType) {
      keySerde = getAvroSerde(parentType.getKeyType(), runtime.getConfiguration());
      valueSerde = getAvroSerde(parentType.getValueType(), runtime.getConfiguration());
    } else {
      keySerde = new WritableSerDe(((WritableType) parentType.getKeyType()).getSerializationClass());
      valueSerde = new WritableSerDe(((WritableType) parentType.getValueType()).getSerializationClass());
    }

    int numPartitions = (groupingOptions.getNumReducers() > 0) ? groupingOptions.getNumReducers() :
        PartitionUtils.getRecommendedPartitions(this, getPipeline().getConfiguration());
    if (numPartitions <= 0) {
      LOG.warn("Attempted to set a non-positive number of partitions");
      numPartitions = 1;
    }

    JavaPairRDD<ByteArray, List<byte[]>> groupedRDD;
    if (groupingOptions.getPartitionerClass() != null) {
      groupedRDD = parentRDD
          .map(new PairMapFunction(ptype.getOutputMapFn(), runtime.getRuntimeContext()))
          .mapToPair(
              new PartitionedMapOutputFunction(keySerde, valueSerde, ptype, groupingOptions.getPartitionerClass(),
              numPartitions, runtime.getRuntimeContext()))
          .groupByKey(new SparkPartitioner(numPartitions));
    } else {
      groupedRDD = parentRDD
          .map(new PairMapFunction(ptype.getOutputMapFn(), runtime.getRuntimeContext()))
          .mapToPair(new MapOutputFunction(keySerde, valueSerde))
          .groupByKey(numPartitions);
    }

    if (groupingOptions.requireSortedKeys() || groupingOptions.getSortComparatorClass() != null) {
      SparkComparator scmp = new SparkComparator(groupingOptions, ptype, runtime.getRuntimeContext());
      groupedRDD = groupedRDD.sortByKey(scmp);
    }
    if (groupingOptions.getGroupingComparatorClass() != null) {
      groupedRDD = groupedRDD.mapPartitionsToPair(
          new ReduceGroupingFunction(groupingOptions, ptype, runtime.getRuntimeContext()));
    }

    return groupedRDD
        .map(new ReduceInputFunction(keySerde, valueSerde))
        .mapToPair(new PairMapIterableFunction(ptype.getInputMapFn(), runtime.getRuntimeContext()));
  }
}
