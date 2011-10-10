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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.type.PGroupedTableType;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;

/**
 *
 *
 */
public class AvroTableType<K, V> extends AvroType<Pair<K, V>> implements PTableType<K, V> {
  
  private static class PairToAvroPair extends MapFn<Pair, org.apache.avro.mapred.Pair> {
    private final MapFn keyMapFn;
    private final MapFn valueMapFn;
    private final String firstJson;
    private final String secondJson;
    
    private Schema pairSchema;
    
    public PairToAvroPair(AvroType keyType, AvroType valueType) {
      this.keyMapFn = keyType.getBaseOutputMapFn();
      this.firstJson = keyType.getSchema().toString();
      this.valueMapFn = valueType.getBaseOutputMapFn();
      this.secondJson = valueType.getSchema().toString();
    }
    
    @Override
    public void initialize() {
      keyMapFn.initialize();
      valueMapFn.initialize();
      pairSchema = org.apache.avro.mapred.Pair.getPairSchema(
          new Schema.Parser().parse(firstJson),
          new Schema.Parser().parse(secondJson));
    }
    
    @Override
    public org.apache.avro.mapred.Pair map(Pair input) {
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
    super(pairClass,
        org.apache.avro.mapred.Pair.getPairSchema(keyType.getSchema(), valueType.getSchema()),
        new IndexedRecordToPair(keyType.getBaseInputMapFn(), valueType.getBaseInputMapFn()),
        new PairToAvroPair(keyType, valueType), keyType, valueType);
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
}
