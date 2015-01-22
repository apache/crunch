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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.PairMapFn;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.seq.SeqFileSource;
import org.apache.crunch.io.seq.SeqFileTableSource;
import org.apache.crunch.io.seq.SeqFileTableSourceTarget;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
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
  public ReadableSource<Pair<K, V>> createSourceTarget(
          Configuration conf, Path path, Iterable<Pair<K, V>> contents, int parallelism) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    outputFn.setConfiguration(conf);
    outputFn.initialize();
    fs.mkdirs(path);
    List<SequenceFile.Writer> writers = Lists.newArrayListWithExpectedSize(parallelism);
    for (int i = 0; i < parallelism; i++) {
      Path out = new Path(path, "out" + i);
      writers.add(SequenceFile.createWriter(fs, conf, out, keyType.getSerializationClass(),
              valueType.getSerializationClass()));
    }
    int target = 0;
    for (Pair<K, V> value : contents) {
      Pair writablePair = (Pair) outputFn.map(value);
      writers.get(target).append(writablePair.first(), writablePair.second());
      target = (target + 1) % parallelism;
    }
    for (SequenceFile.Writer writer : writers) {
      writer.close();
    }
    ReadableSource<Pair<K, V>> ret = new SeqFileTableSource<K, V>(path, this);
    ret.inputConf(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    return ret;
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