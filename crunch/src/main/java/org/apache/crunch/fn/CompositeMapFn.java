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
import org.apache.hadoop.conf.Configuration;

public class CompositeMapFn<R, S, T> extends MapFn<R, T> {

  private final MapFn<R, S> first;
  private final MapFn<S, T> second;

  public CompositeMapFn(MapFn<R, S> first, MapFn<S, T> second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void initialize() {
    first.setContext(getContext());
    second.setContext(getContext());
  }

  public MapFn<R, S> getFirst() {
    return first;
  }

  public MapFn<S, T> getSecond() {
    return second;
  }

  @Override
  public T map(R input) {
    return second.map(first.map(input));
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    first.cleanup(null);
    second.cleanup(null);
  }

  @Override
  public void configure(Configuration conf) {
    first.configure(conf);
    second.configure(conf);
  }

  @Override
  public void setConfigurationForTest(Configuration conf) {
    first.setConfigurationForTest(conf);
    second.setConfigurationForTest(conf);
  }
}
