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

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;

import com.cloudera.crunch.types.Converter;

public class AvroKeyConverter<K> implements Converter<AvroWrapper<K>, NullWritable, K, Iterable<K>> {
  private transient AvroWrapper<K> wrapper = null;
  
  @Override
  public K convertInput(AvroWrapper<K> key, NullWritable value) {
    return key.datum();
  }

  @Override
  public AvroWrapper<K> outputKey(K value) {
    getWrapper().datum(value);
    return wrapper;
  }

  @Override
  public NullWritable outputValue(K value) {
    return NullWritable.get();
  }

  @Override
  public Class<AvroWrapper<K>> getKeyClass() {
    return (Class<AvroWrapper<K>>) getWrapper().getClass();
  }

  @Override
  public Class<NullWritable> getValueClass() {
    return NullWritable.class;
  }

  private AvroWrapper<K> getWrapper() {
    if (wrapper == null) {
      wrapper = new AvroWrapper<K>();
    }
    return wrapper;
  }

  @Override
  public Iterable<K> convertIterableInput(AvroWrapper<K> key,
      Iterable<NullWritable> value) {
    throw new UnsupportedOperationException("Should not be possible");
  }
}
