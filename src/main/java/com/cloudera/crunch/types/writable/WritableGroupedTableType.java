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
package com.cloudera.crunch.types.writable;

import com.cloudera.crunch.types.Converter;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PGroupedTableType;

public class WritableGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private final MapFn inputFn;
  private final MapFn outputFn;
  private final Converter converter;
  
  public WritableGroupedTableType(WritableTableType<K, V> tableType) {
    super(tableType);
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    this.inputFn =  new PairIterableMapFn(keyType.getInputMapFn(),
        valueType.getInputMapFn());
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
  public void configureShuffle(Job job, GroupingOptions options) {
    if (options != null) {
      options.configure(job);
    }
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    job.setMapOutputKeyClass(keyType.getSerializationClass());
    job.setMapOutputValueClass(valueType.getSerializationClass());
  }
}
