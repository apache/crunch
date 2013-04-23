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

import org.apache.avro.mapred.AvroWrapper;
import org.apache.crunch.types.Converter;
import org.apache.hadoop.io.NullWritable;

class AvroKeyConverter<K> implements Converter<AvroWrapper<K>, NullWritable, K, Iterable<K>> {

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
  public Iterable<K> convertIterableInput(AvroWrapper<K> key, Iterable<NullWritable> value) {
    throw new UnsupportedOperationException("Should not be possible");
  }
}
