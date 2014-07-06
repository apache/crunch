/*
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.crunch.types.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.TupleDeepCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * A {@code PTableType} that is compatible with Avro key/value files that are created or read using the
 * {@code org.apache.avro.mapreduce.AvroJob} class.
 */
class AvroKeyValueTableType<K, V> extends BaseAvroTableType<K, V> implements PTableType<K, V> {

  private static class PairToAvroKeyValueRecord extends MapFn<Pair, GenericRecord> {
    private final MapFn keyMapFn;
    private final MapFn valueMapFn;
    private final String keySchemaJson;
    private final String valueSchemaJson;

    private String keyValueSchemaJson;
    private transient Schema keyValueSchema;

    public PairToAvroKeyValueRecord(AvroType keyType, AvroType valueType) {
      this.keyMapFn = keyType.getOutputMapFn();
      this.keySchemaJson = keyType.getSchema().toString();
      this.valueMapFn = valueType.getOutputMapFn();
      this.valueSchemaJson = valueType.getSchema().toString();
    }

    @Override
    public void configure(Configuration conf) {
      keyMapFn.configure(conf);
      valueMapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      keyMapFn.setContext(context);
      valueMapFn.setContext(context);
    }

    @Override
    public void initialize() {
      keyMapFn.initialize();
      valueMapFn.initialize();
      Schema.Parser parser = new Schema.Parser();
      keyValueSchemaJson = AvroKeyValue.getSchema(parser.parse(keySchemaJson), parser.parse(valueSchemaJson)).toString();
    }

    @Override
    public GenericRecord map(Pair input) {
      if (keyValueSchema == null) {
        keyValueSchema = new Schema.Parser().parse(keyValueSchemaJson);
      }
      GenericRecord keyValueRecord = new GenericData.Record(keyValueSchema);
      keyValueRecord.put(AvroKeyValue.KEY_FIELD, keyMapFn.map(input.first()));
      keyValueRecord.put(AvroKeyValue.VALUE_FIELD, valueMapFn.map(input.second()));
      return keyValueRecord;
    }
  }

  private static class AvroKeyValueRecordToPair extends MapFn<GenericRecord, Pair> {

    private final MapFn firstMapFn;
    private final MapFn secondMapFn;

    public AvroKeyValueRecordToPair(MapFn firstMapFn, MapFn secondMapFn) {
      this.firstMapFn = firstMapFn;
      this.secondMapFn = secondMapFn;
    }

    @Override
    public void configure(Configuration conf) {
      firstMapFn.configure(conf);
      secondMapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      firstMapFn.setContext(context);
      secondMapFn.setContext(context);
    }

    @Override
    public void initialize() {
      firstMapFn.initialize();
      secondMapFn.initialize();
    }

    @Override
    public Pair map(GenericRecord input) {
      return Pair.of(
          firstMapFn.map(input.get(AvroKeyValue.KEY_FIELD)),
          secondMapFn.map(input.get(AvroKeyValue.VALUE_FIELD)));
    }
  }

  private final AvroType<K> keyType;
  private final AvroType<V> valueType;

  public AvroKeyValueTableType(AvroType<K> keyType, AvroType<V> valueType, Class<Pair<K, V>> pairClass) {
    super(pairClass, AvroKeyValue.getSchema(keyType.getSchema(), valueType.getSchema()),
        new AvroKeyValueRecordToPair(keyType.getInputMapFn(), valueType.getInputMapFn()),
        new PairToAvroKeyValueRecord(keyType, valueType),
        new TupleDeepCopier(Pair.class, keyType, valueType),
        null, keyType, valueType);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public PType<K> getKeyType() {
    return keyType;
  }

  @Override
  public PType<V> getValueType() {
    return valueType;
  }

  @Override
  public PGroupedTableType<K, V> getGroupedTableType() {
    return new AvroGroupedTableType<K, V>(this);
  }

  @Override
  public Pair<K, V> getDetachedValue(Pair<K, V> value) {
    return PTables.getDetachedValue(this, value);
  }
}
