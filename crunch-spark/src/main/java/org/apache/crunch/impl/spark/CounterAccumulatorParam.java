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
package org.apache.crunch.impl.spark;

import com.google.common.collect.Maps;
import org.apache.spark.AccumulatorParam;

import java.util.Map;

public class CounterAccumulatorParam implements AccumulatorParam<Map<String, Map<String, Long>>> {
  @Override
  public Map<String, Map<String, Long>> addAccumulator(
      Map<String, Map<String, Long>> current,
      Map<String, Map<String, Long>> added) {
    for (Map.Entry<String, Map<String, Long>> e : added.entrySet()) {
      Map<String, Long> grp = current.get(e.getKey());
      if (grp == null) {
        grp = Maps.newTreeMap();
        current.put(e.getKey(), grp);
      }
      for (Map.Entry<String, Long> f : e.getValue().entrySet()) {
        Long cnt = grp.get(f.getKey());
        cnt = (cnt == null) ? f.getValue() : cnt + f.getValue();
        grp.put(f.getKey(), cnt);
      }
    }
    return current;
  }

  @Override
  public Map<String, Map<String, Long>> addInPlace(
      Map<String, Map<String, Long>> first,
      Map<String, Map<String, Long>> second) {
    return addAccumulator(first, second);
  }

  @Override
  public Map<String, Map<String, Long>> zero(Map<String, Map<String, Long>> counts) {
    return Maps.newHashMap();
  }
}
