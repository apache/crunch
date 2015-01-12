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
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Map;

import static org.apache.crunch.types.avro.Avros.*;
import static org.junit.Assert.assertEquals;

public class AverageTest {
  @Test
  public void testMeanValue() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 2,
            "a", 10,
            "b", 3,
            "c", 3,
            "c", 4,
            "c", 5);
    Map<String, Double> actual = Average.meanValue(testTable).materializeToMap();
    Map<String, Double> expected = ImmutableMap.of(
            "a", 6.0,
            "b", 3.0,
            "c", 4.0
    );

    assertEquals(expected, actual);
  }


}