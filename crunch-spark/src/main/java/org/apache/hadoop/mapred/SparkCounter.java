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
package org.apache.hadoop.mapred;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.Accumulator;

import java.util.Map;

public class SparkCounter extends Counters.Counter {

  private String group;
  private String name;
  private long value = 0;
  private Accumulator<Map<String, Map<String, Long>>> accum;

  public SparkCounter(String group, String name, Accumulator<Map<String, Map<String, Long>>> accum) {
    this.group = group;
    this.name = name;
    this.accum = accum;
  }

  public SparkCounter(String group, String name, long value) {
    this.group = group;
    this.name = name;
    this.value = value;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDisplayName() {
    return name;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public long getCounter() {
    return getValue();
  }

  @Override
  public void increment(long inc) {
    this.value += inc;
    accum.add(ImmutableMap.<String, Map<String, Long>>of(group, ImmutableMap.of(name, inc)));
  }

  @Override
  public void setValue(long newValue) {
    long delta = newValue - value;
    accum.add(ImmutableMap.<String, Map<String, Long>>of(group, ImmutableMap.of(name, delta)));
    this.value = newValue;
  }
}
