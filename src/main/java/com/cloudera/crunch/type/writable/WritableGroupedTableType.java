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
package com.cloudera.crunch.type.writable;

import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.type.DataBridge;
import com.cloudera.crunch.type.PGroupedTableType;

public class WritableGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private final DataBridge handler;
  
  public WritableGroupedTableType(WritableTableType<K, V> tableType) {
    super(tableType);
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    MapFn mapFn =  new PairIterableMapFn(keyType.getDataBridge().getInputMapFn(),
        valueType.getDataBridge().getInputMapFn());
    this.handler = DataBridge.forPair(keyType.getSerializationClass(), valueType.getSerializationClass(),
        mapFn, tableType.getDataBridge().getOutputMapFn());
  }
  
  @Override
  public Class<Pair<K, Iterable<V>>> getTypeClass() {
    return (Class<Pair<K, Iterable<V>>>) Pair.of(null, null).getClass();  
  }
  
  @Override
  public DataBridge getGroupingBridge() {
    return handler;
  }

  @Override
  public void configureShuffle(Job job, GroupingOptions options) {
    if (options != null) {
      options.configure(job);
    }
    job.setMapOutputKeyClass(handler.getKeyClass());
    job.setMapOutputValueClass(handler.getValueClass());
  }
}
