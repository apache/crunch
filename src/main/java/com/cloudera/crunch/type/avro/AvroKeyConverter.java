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

import org.apache.hadoop.io.NullWritable;

import com.cloudera.crunch.type.Converter;

/**
 *
 *
 */
public class AvroKeyConverter<K> implements Converter<K, Object, K> {
  @Override
  public K convertInput(K key, Object value) {
    return key;
  }

  @Override
  public K outputKey(K value) {
    return value;
  }

  @Override
  public Object outputValue(K value) {
    return NullWritable.get();
  }

}
