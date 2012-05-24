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
package com.cloudera.crunch.types.avro;

import java.util.Collection;

import com.cloudera.crunch.types.Converter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.fn.PairMapFn;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PGroupedTableType;

/**
 *
 *
 */
public class AvroGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private static final AvroPairConverter CONVERTER = new AvroPairConverter();
  private final MapFn inputFn;
  private final MapFn outputFn;
  
  public AvroGroupedTableType(AvroTableType<K, V> tableType) {
    super(tableType);
    AvroType keyType = (AvroType) tableType.getKeyType();
    AvroType valueType = (AvroType) tableType.getValueType();
    this.inputFn =  new PairIterableMapFn(keyType.getInputMapFn(),
        valueType.getInputMapFn());
    this.outputFn = new PairMapFn(keyType.getOutputMapFn(),
        valueType.getOutputMapFn());
  }

  @Override
  public Class<Pair<K, Iterable<V>>> getTypeClass() {
    return (Class<Pair<K, Iterable<V>>>) Pair.of(null, null).getClass();  
  }

  @Override
  public Converter getGroupingConverter() {
    return CONVERTER;
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
    AvroTableType<K, V> att = (AvroTableType<K, V>) tableType;
    String schemaJson = att.getSchema().toString();
    Configuration conf = job.getConfiguration();
    
    if (!att.isSpecific()) {
        conf.setBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, true);
    }
    conf.set(AvroJob.MAP_OUTPUT_SCHEMA, schemaJson);
    job.setSortComparatorClass(AvroKeyComparator.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    if (options != null) {
      options.configure(job);
    }
    
    Avros.configureReflectDataFactory(conf);
    
    Collection<String> serializations =
        job.getConfiguration().getStringCollection("io.serializations");
    if (!serializations.contains(SafeAvroSerialization.class.getName())) {
      serializations.add(SafeAvroSerialization.class.getName());
      job.getConfiguration().setStrings("io.serializations",
          serializations.toArray(new String[0]));
    }
  }
  
}
