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
package org.apache.crunch.types.writable;

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

class WritableGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private final MapFn inputFn;
  private final MapFn outputFn;
  private final Converter converter;

  WritableGroupedTableType(WritableTableType<K, V> tableType) {
    super(tableType);
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    this.inputFn = new PairIterableMapFn(keyType.getInputMapFn(), valueType.getInputMapFn());
    this.outputFn = tableType.getOutputMapFn();
    this.converter = new WritablePairConverter(keyType.getSerializationClass(),
        valueType.getSerializationClass());
  }

  @Override
  public Class<Pair<K, Iterable<V>>> getTypeClass() {
    return (Class<Pair<K, Iterable<V>>>) Pair.of(null, null).getClass();
  }

  @Override
  public Converter getGroupingConverter() {
    return converter;
  }

  @Override
  public MapFn getInputMapFn() {
    return inputFn;
  }

  @Override
  public MapFn getOutputMapFn() {
    return outputFn;
  }

  @Override
  public void initialize(Configuration conf) {
    this.tableType.initialize(conf);
  }

  @Override
  public Pair<K, Iterable<V>> getDetachedValue(Pair<K, Iterable<V>> value) {
    return PTables.getGroupedDetachedValue(this, value);
  }

  @Override
  public ReadableSource<Pair<K, Iterable<V>>> createSourceTarget(
      Configuration conf,
      Path path,
      Iterable<Pair<K, Iterable<V>>> contents,
      int parallelism) throws IOException {
    throw new UnsupportedOperationException("GroupedTableTypes do not support creating ReadableSources");
  }

  @Override
  public void configureShuffle(Job job, GroupingOptions options) {
    if (options != null) {
      options.configure(job);
    }
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    job.setMapOutputKeyClass(keyType.getSerializationClass());
    job.setMapOutputValueClass(valueType.getSerializationClass());
    if ((options == null || options.getSortComparatorClass() == null) &&
        TupleWritable.class.equals(keyType.getSerializationClass())) {
      job.setSortComparatorClass(TupleWritable.Comparator.class);
    }
  }
}
