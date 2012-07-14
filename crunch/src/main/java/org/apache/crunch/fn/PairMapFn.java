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

import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;

public class PairMapFn<K, V, S, T> extends MapFn<Pair<K, V>, Pair<S, T>> {

  private MapFn<K, S> keys;
  private MapFn<V, T> values;

  public PairMapFn(MapFn<K, S> keys, MapFn<V, T> values) {
    this.keys = keys;
    this.values = values;
  }

  @Override
  public void configure(Configuration conf) {
    keys.configure(conf);
    values.configure(conf);
  }

  @Override
  public void initialize() {
    keys.setContext(getContext());
    values.setContext(getContext());
  }

  @Override
  public Pair<S, T> map(Pair<K, V> input) {
    return Pair.of(keys.map(input.first()), values.map(input.second()));
  }

  @Override
  public void cleanup(Emitter<Pair<S, T>> emitter) {
    keys.cleanup(null);
    values.cleanup(null);
  }

  @Override
  public void setConfigurationForTest(Configuration conf) {
    keys.setConfigurationForTest(conf);
    values.setConfigurationForTest(conf);
  }
}