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
package org.apache.crunch.fn;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Wrapper function for converting a {@code MapFn} into a key-value pair that is
 * used to convert from a {@code PCollection<V>} to a {@code PTable<K, V>}.
 */
public class ExtractKeyFn<K, V> extends MapFn<V, Pair<K, V>> {

  private final MapFn<V, K> mapFn;

  public ExtractKeyFn(MapFn<V, K> mapFn) {
    this.mapFn = mapFn;
  }

  @Override
  public void setConfiguration(Configuration conf) {
    mapFn.setConfiguration(conf);
  }

  @Override
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    mapFn.setContext(context);
  }

  @Override
  public void configure(Configuration conf) {
    mapFn.configure(conf);
  }

  @Override
  public void initialize() {
    mapFn.initialize();
  }

  @Override
  public Pair<K, V> map(V input) {
    return Pair.of(mapFn.map(input), input);
  }
}
