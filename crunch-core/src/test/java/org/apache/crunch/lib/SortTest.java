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

import com.google.common.collect.ImmutableList;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import static org.apache.crunch.types.avro.Avros.longs;
import static org.apache.crunch.types.avro.Avros.pairs;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.junit.Assert.assertEquals;

public class SortTest {

  @Test
  public void testInMemoryReverseAvro() throws Exception {
    PCollection<Pair<String, Long>> pc = MemPipeline.typedCollectionOf(pairs(strings(), longs()),
        Pair.of("a", 1L), Pair.of("c", 7L), Pair.of("b", 10L));
    PCollection<Pair<String, Long>> sorted = Sort.sortPairs(pc, Sort.ColumnOrder.by(2, Sort.Order.DESCENDING));
    assertEquals(ImmutableList.of(Pair.of("b", 10L), Pair.of("c", 7L), Pair.of("a", 1L)),
        ImmutableList.copyOf(sorted.materialize()));
  }
}
