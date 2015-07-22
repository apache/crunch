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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
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
 * The implementation of the PTableType interface for Avro-based serialization.
 * 
 */
class AvroTableType<K, V> extends BaseAvroTableType<K, V> implements PTableType<K, V> {

  private static class PairToAvroPair extends MapFn<Pair, org.apache.avro.mapred.Pair> {
    private final MapFn keyMapFn;
    private final MapFn valueMapFn;
    private final String firstJson;
    private final String secondJson;

    private String pairSchemaJson;
    private transient Schema pairSchema;

    public PairToAvroPair(AvroType keyType, AvroType valueType) {
      this.keyMapFn = keyType.getOutputMapFn();
      this.firstJson = keyType.getSchema().toString();
      this.valueMapFn = valueType.getOutputMapFn();
      this.secondJson = valueType.getSchema().toString();
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
      pairSchemaJson = org.apache.avro.mapred.Pair.getPairSchema(
          new Schema.Parser().parse(firstJson),
          Avros.allowNulls(new Schema.Parser().parse(secondJson))).toString();
    }

    @Override
    public org.apache.avro.mapred.Pair map(Pair input) {
      if (pairSchema == null) {
        pairSchema = new Schema.Parser().parse(pairSchemaJson);
      }
      org.apache.avro.mapred.Pair avroPair = new org.apache.avro.mapred.Pair(pairSchema);
      avroPair.key(keyMapFn.map(input.first()));
      avroPair.value(valueMapFn.map(input.second()));
      return avroPair;
    }
  }

  private static class IndexedRecordToPair extends MapFn<IndexedRecord, Pair> {

    private final MapFn firstMapFn;
    private final MapFn secondMapFn;

    public IndexedRecordToPair(MapFn firstMapFn, MapFn secondMapFn) {
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
    public Pair map(IndexedRecord input) {
      return Pair.of(firstMapFn.map(input.get(0)), secondMapFn.map(input.get(1)));
    }
  }

  private final AvroType<K> keyType;
  private final AvroType<V> valueType;

  public AvroTableType(AvroType<K> keyType, AvroType<V> valueType, Class<Pair<K, V>> pairClass) {
    super(pairClass, org.apache.avro.mapred.Pair.getPairSchema(keyType.getSchema(),
            Avros.allowNulls(valueType.getSchema())),
        new IndexedRecordToPair(keyType.getInputMapFn(),
        valueType.getInputMapFn()), new PairToAvroPair(keyType, valueType),
        new TupleDeepCopier(Pair.class, keyType, valueType), null, keyType, valueType);
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
