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

import java.util.Collections;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CartesianTest {

  @Test
  public void testCartesianCollection_SingleValues() {

    PCollection<String> letters = MemPipeline.typedCollectionOf(Writables.strings(), "a", "b");
    PCollection<Integer> ints = MemPipeline.typedCollectionOf(Writables.ints(), 1, 2);

    PCollection<Pair<String, Integer>> cartesianProduct = Cartesian.cross(letters, ints);

    @SuppressWarnings("unchecked")
    List<Pair<String, Integer>> expectedResults = Lists.newArrayList(Pair.of("a", 1), Pair.of("a", 2), Pair.of("b", 1),
        Pair.of("b", 2));
    List<Pair<String, Integer>> actualResults = Lists.newArrayList(cartesianProduct.materialize());
    Collections.sort(actualResults);

    assertEquals(expectedResults, actualResults);
  }

  @Test
  public void testCartesianCollection_Tables() {

    PTable<String, Integer> leftTable = MemPipeline.typedTableOf(
        Writables.tableOf(Writables.strings(), Writables.ints()), "a", 1, "b", 2);
    PTable<String, Float> rightTable = MemPipeline.typedTableOf(
        Writables.tableOf(Writables.strings(), Writables.floats()), "A", 1.0f, "B", 2.0f);

    PTable<Pair<String, String>, Pair<Integer, Float>> cartesianProduct = Cartesian.cross(leftTable, rightTable);

    List<Pair<Pair<String, String>, Pair<Integer, Float>>> expectedResults = Lists.newArrayList();
    expectedResults.add(Pair.of(Pair.of("a", "A"), Pair.of(1, 1.0f)));
    expectedResults.add(Pair.of(Pair.of("a", "B"), Pair.of(1, 2.0f)));
    expectedResults.add(Pair.of(Pair.of("b", "A"), Pair.of(2, 1.0f)));
    expectedResults.add(Pair.of(Pair.of("b", "B"), Pair.of(2, 2.0f)));

    List<Pair<Pair<String, String>, Pair<Integer, Float>>> actualResults = Lists.newArrayList(cartesianProduct
        .materialize());
    Collections.sort(actualResults);

    assertEquals(expectedResults, actualResults);

  }

}
