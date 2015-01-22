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
package org.apache.crunch.types.avro;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroValue;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.PairMapFn;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 *
 */
class AvroGroupedTableType<K, V> extends PGroupedTableType<K, V> {

  private static final AvroPairConverter CONVERTER = new AvroPairConverter();
  private final MapFn inputFn;
  private final MapFn outputFn;

  public AvroGroupedTableType(BaseAvroTableType<K, V> tableType) {
    super(tableType);
    AvroType keyType = (AvroType) tableType.getKeyType();
    AvroType valueType = (AvroType) tableType.getValueType();
    this.inputFn = new PairIterableMapFn(keyType.getInputMapFn(), valueType.getInputMapFn());
    this.outputFn = new PairMapFn(keyType.getOutputMapFn(), valueType.getOutputMapFn());
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
  public void initialize(Configuration conf) {
    getTableType().initialize(conf);
  }

  @Override
  public Pair<K, Iterable<V>> getDetachedValue(Pair<K, Iterable<V>> value) {
    return PTables.getGroupedDetachedValue(this, value);
  }

  @Override
  public void configureShuffle(Job job, GroupingOptions options) {
    AvroTableType<K, V> att = (AvroTableType<K, V>) tableType;
    String schemaJson = att.getSchema().toString();
    Configuration conf = job.getConfiguration();

    if (att.hasReflect()) {
      if (att.hasSpecific()) {
        Avros.checkCombiningSpecificAndReflectionSchemas();
      }
      conf.setBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, true);
    }
    conf.set(AvroJob.MAP_OUTPUT_SCHEMA, schemaJson);
    job.setSortComparatorClass(AvroKeyComparator.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    if (options != null) {
      options.configure(job);
    }

    AvroMode.fromType(att).withFactoryFromConfiguration(conf).configureShuffle(conf);

    Collection<String> serializations = job.getConfiguration().getStringCollection(
        "io.serializations");
    if (!serializations.contains(SafeAvroSerialization.class.getName())) {
      serializations.add(SafeAvroSerialization.class.getName());
      job.getConfiguration().setStrings("io.serializations", serializations.toArray(new String[0]));
    }
  }

  @Override
  public ReadableSource<Pair<K, Iterable<V>>> createSourceTarget(
          Configuration conf,
          Path path,
          Iterable<Pair<K, Iterable<V>>> contents,
          int parallelism) throws IOException {
    throw new UnsupportedOperationException("GroupedTableTypes do not support creating ReadableSources");
  }
}
