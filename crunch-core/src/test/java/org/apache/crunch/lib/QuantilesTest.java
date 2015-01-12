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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Quantiles.Result;

import org.junit.Test;

import java.util.Map;

import static org.apache.crunch.types.avro.Avros.*;
import static org.junit.Assert.assertEquals;

public class QuantilesTest {

  private static <T> Quantiles.Result<T> result(long count, Pair<Double, T>... quantiles) {
    return new Quantiles.Result<T>(count, Lists.newArrayList(quantiles));
  }

  @Test
  public void testQuantilesExact() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2,
            "a", 3,
            "a", 4,
            "a", 1);
    Map<String, Result<Integer>> actualS = Quantiles.distributed(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Result<Integer>> actualM = Quantiles.inMemory(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(5, Pair.of(0.0, 1), Pair.of(0.5, 3), Pair.of(1.0, 5))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testQuantilesBetween() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2, // We expect the 0.5 to correspond to this element, according to the "nearest rank" %ile definition.
            "a", 4,
            "a", 1);
    Map<String, Result<Integer>> actualS = Quantiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> actualM = Quantiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(4, Pair.of(0.5, 2))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testQuantilesNines() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 10,
            "a", 20,
            "a", 30,
            "a", 40,
            "a", 50,
            "a", 60,
            "a", 70,
            "a", 80,
            "a", 90,
            "a", 100);
    Map<String, Result<Integer>> actualS = Quantiles.distributed(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Result<Integer>> actualM = Quantiles.inMemory(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(10, Pair.of(0.9, 90), Pair.of(0.99, 100))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testQuantilesLessThanOrEqual() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 10,
            "a", 20,
            "a", 30,
            "a", 40,
            "a", 50,
            "a", 60,
            "a", 70,
            "a", 80,
            "a", 90,
            "a", 100);
    Map<String, Result<Integer>> actualS = Quantiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> actualM = Quantiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(10, Pair.of(0.5, 50))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }


}
