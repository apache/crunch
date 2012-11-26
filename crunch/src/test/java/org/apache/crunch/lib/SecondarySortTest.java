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

import static org.apache.crunch.types.avro.Avros.*;
import static org.junit.Assert.assertEquals;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import com.google.common.collect.ImmutableList;


public class SecondarySortTest {
  @Test
  public void testInMemory() throws Exception {
    PTable<Long, Pair<Long, String>> input = MemPipeline.typedTableOf(tableOf(longs(), pairs(longs(), strings())),
        1729L, Pair.of(17L, "a"), 100L, Pair.of(29L, "b"), 1729L, Pair.of(29L, "c"));
    PCollection<String> letters = SecondarySort.sortAndApply(input, new StringifyFn(), strings());
    assertEquals(ImmutableList.of("b", "ac"), letters.materialize());
  }
  
  private static class StringifyFn extends DoFn<Pair<Long, Iterable<Pair<Long, String>>>, String> {
    @Override
    public void process(Pair<Long, Iterable<Pair<Long, String>>> input, Emitter<String> emitter) {
      StringBuilder sb = new StringBuilder();
      for (Pair<Long, String> p : input.second()) {
        sb.append(p.second());
      }
      emitter.emit(sb.toString());
    }
  }
}
