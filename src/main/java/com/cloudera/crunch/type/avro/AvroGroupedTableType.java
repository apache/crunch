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
package com.cloudera.crunch.type.avro;

import java.util.Collection;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.fn.PairMapFn;
import com.cloudera.crunch.type.DataBridge;
import com.cloudera.crunch.type.PGroupedTableType;

/**
 *
 *
 */
public class AvroGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private final DataBridge handler;
  
  public AvroGroupedTableType(AvroTableType<K, V> tableType) {
    super(tableType);
    AvroType keyType = (AvroType) tableType.getKeyType();
    AvroType valueType = (AvroType) tableType.getValueType();
    DataBridge keyHandler = keyType.getDataBridge();
    DataBridge valueHandler = valueType.getDataBridge();
    MapFn inputMapFn =  new PairIterableMapFn(keyHandler.getInputMapFn(),
        valueHandler.getInputMapFn());
    MapFn outputMapFn = new PairMapFn(new AvroKeyMapFn(keyType.getBaseOutputMapFn()),
        new AvroValueMapFn(valueType.getBaseOutputMapFn()));
    this.handler = DataBridge.forPair(AvroKey.class, AvroValue.class,
        inputMapFn, outputMapFn);

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
    AvroTableType<K, V> att = (AvroTableType<K, V>) tableType;
    String schemaJson = att.getSchema().toString();
    job.getConfiguration().set(AvroJob.MAP_OUTPUT_SCHEMA, schemaJson);
    job.setSortComparatorClass(AvroKeyComparator.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    if (options != null) {
      options.configure(job);
    }
    
    Collection<String> serializations =
        job.getConfiguration().getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      job.getConfiguration().setStrings("io.serializations",
          serializations.toArray(new String[0]));
    }
  }
  
  private static class AvroKeyMapFn<S, T> extends MapFn<S, AvroWrapper<T>> {
    private final MapFn<S, T> map;
    private transient AvroKey<T> wrapper;
    
    public AvroKeyMapFn(MapFn<S, T> map) {
      this.map = map;
    }
    
    @Override
    public void initialize() {
      this.wrapper = new AvroKey<T>();
      this.map.initialize();
    }
    
    @Override
    public AvroWrapper<T> map(S input) {
      wrapper.datum(map.map(input));
      return wrapper;
    } 
  }

  private static class AvroValueMapFn<S, T> extends MapFn<S, AvroWrapper<T>> {
    private final MapFn<S, T> map;
    private transient AvroValue<T> wrapper;
    
    public AvroValueMapFn(MapFn<S, T> map) {
      this.map = map;
    }
    
    @Override
    public void initialize() {
      this.wrapper = new AvroValue<T>();
      this.map.initialize();
    }
    
    @Override
    public AvroWrapper<T> map(S input) {
      wrapper.datum(map.map(input));
      return wrapper;
    } 
  }
}
