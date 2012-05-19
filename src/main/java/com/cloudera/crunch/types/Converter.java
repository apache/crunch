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
package com.cloudera.crunch.types;

import java.io.Serializable;

import com.cloudera.crunch.DoFn;

/**
 * Converts the input key/value from a MapReduce task into the input to a
 * {@link DoFn}, or takes the output of a {@code DoFn} and write it to
 * the output key/values.
 */
public interface Converter<K, V, S, T> extends Serializable {
  S convertInput(K key, V value);
  
  T convertIterableInput(K key, Iterable<V> value);
  
  K outputKey(S value);
  
  V outputValue(S value);
  
  Class<K> getKeyClass();
  
  Class<V> getValueClass();
}
