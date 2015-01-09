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
package org.apache.crunch.impl.spark.fn;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.IntByteArray;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

public class PartitionedMapOutputFunction<K, V> implements PairFunction<Pair<K, V>, IntByteArray, byte[]> {

  private final SerDe<K> keySerde;
  private final SerDe<V> valueSerde;
  private final PGroupedTableType<K, V> ptype;
  private final Class<? extends Partitioner> partitionerClass;
  private final int numPartitions;
  private final SparkRuntimeContext runtimeContext;
  private transient Partitioner partitioner;

  public PartitionedMapOutputFunction(
    SerDe<K> keySerde,
    SerDe<V> valueSerde,
    PGroupedTableType<K, V> ptype,
    Class<? extends Partitioner> partitionerClass,
    int numPartitions,
    SparkRuntimeContext runtimeContext) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.ptype = ptype;
    this.partitionerClass = partitionerClass;
    this.numPartitions = numPartitions;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public Tuple2<IntByteArray, byte[]> call(Pair<K, V> p) throws Exception {
    int partition = getPartitioner().getPartition(p.first(), p.second(), numPartitions);
    return new Tuple2<IntByteArray, byte[]>(
        new IntByteArray(partition, keySerde.toBytes(p.first())),
        valueSerde.toBytes(p.second()).value);
  }

  private Partitioner getPartitioner() {
    if (partitioner == null) {
      try {
        ptype.initialize(runtimeContext.getConfiguration());
        Job job = new Job(runtimeContext.getConfiguration());
        ptype.configureShuffle(job, GroupingOptions.builder().partitionerClass(partitionerClass).build());
        partitioner = ReflectionUtils.newInstance(partitionerClass, job.getConfiguration());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error configuring partitioner", e);
      }
    }
    return partitioner;
  }
}
