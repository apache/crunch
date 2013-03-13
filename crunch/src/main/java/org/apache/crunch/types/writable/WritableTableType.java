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

import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.PairMapFn;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.seq.SeqFileTableSourceTarget;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.ImmutableList;

class WritableTableType<K, V> implements PTableType<K, V> {

  private final WritableType<K, Writable> keyType;
  private final WritableType<V, Writable> valueType;
  private final MapFn inputFn;
  private final MapFn outputFn;
  private final Converter converter;

  public WritableTableType(WritableType<K, Writable> keyType, WritableType<V, Writable> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.inputFn = new PairMapFn(keyType.getInputMapFn(), valueType.getInputMapFn());
    this.outputFn = new PairMapFn(keyType.getOutputMapFn(), valueType.getOutputMapFn());
    this.converter = new WritablePairConverter(keyType.getSerializationClass(),
        valueType.getSerializationClass());
  }

  @Override
  public Class<Pair<K, V>> getTypeClass() {
    return (Class<Pair<K, V>>) Pair.of(null, null).getClass();
  }

  @Override
  public List<PType> getSubTypes() {
    return ImmutableList.<PType> of(keyType, valueType);
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
  public Converter getConverter() {
    return converter;
  }

  @Override
  public PTypeFamily getFamily() {
    return WritableTypeFamily.getInstance();
  }

  public PType<K> getKeyType() {
    return keyType;
  }

  public PType<V> getValueType() {
    return valueType;
  }

  @Override
  public PGroupedTableType<K, V> getGroupedTableType() {
    return new WritableGroupedTableType<K, V>(this);
  }

  @Override
  public ReadableSourceTarget<Pair<K, V>> getDefaultFileSource(Path path) {
    return new SeqFileTableSourceTarget<K, V>(path, this);
  }

  @Override
  public void initialize(Configuration conf) {
    keyType.initialize(conf);
    valueType.initialize(conf);
  }

  @Override
  public Pair<K, V> getDetachedValue(Pair<K, V> value) {
    return PTables.getDetachedValue(this, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof WritableTableType)) {
      return false;
    }
    WritableTableType that = (WritableTableType) obj;
    return keyType.equals(that.keyType) && valueType.equals(that.valueType);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(keyType).append(valueType).toHashCode();
  }
}