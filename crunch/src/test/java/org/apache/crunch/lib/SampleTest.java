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
package org.apache.crunch.lib;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SampleTest {
  private PCollection<Pair<String, Double>> values = MemPipeline.typedCollectionOf(
      Writables.pairs(Writables.strings(), Writables.doubles()),
      ImmutableList.of(
        Pair.of("foo", 200.0),
        Pair.of("bar", 400.0),
        Pair.of("baz", 100.0),
        Pair.of("biz", 100.0)));
  
  @Test
  public void testWRS() throws Exception {
    Map<String, Integer> histogram = Maps.newHashMap();
    
    for (int i = 0; i < 100; i++) {
      PCollection<String> sample = Sample.weightedReservoirSample(values, 1, 1729L + i);
      for (String s : sample.materialize()) {
        if (!histogram.containsKey(s)) {
          histogram.put(s, 1);
        } else {
          histogram.put(s, 1 + histogram.get(s));
        }
      }
    }
    
    Map<String, Integer> expected = ImmutableMap.of(
        "foo", 24, "bar", 51, "baz", 13, "biz", 12);
    assertEquals(expected, histogram);
  }

  @Test
  public void testSample() {
    PCollection<Integer> pcollect = MemPipeline.collectionOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    Iterable<Integer> sample = Sample.sample(pcollect, 123998L, 0.2).materialize();
    List<Integer> sampleValues = ImmutableList.copyOf(sample);
    assertEquals(ImmutableList.of(6, 7), sampleValues);
  }
}
