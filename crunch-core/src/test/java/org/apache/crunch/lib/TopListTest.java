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
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertEquals;

public class TopListTest {

  private <T> Collection<T> collectionOf(T... elements) {
    return Lists.newArrayList(elements);
  }

  @Test
  public void testTopNYbyX() {
    PTable<String, String> data = MemPipeline.typedTableOf(tableOf(strings(), strings()),
            "a", "x",
            "a", "x",
            "a", "x",
            "a", "y",
            "a", "y",
            "a", "z",
            "b", "x",
            "b", "x",
            "b", "z");
    Map<String, Collection<Pair<Long, String>>> actual = TopList.topNYbyX(data, 2).materializeToMap();
    Map<String, Collection<Pair<Long, String>>> expected = ImmutableMap.of(
            "a", collectionOf(Pair.of(3L, "x"), Pair.of(2L, "y")),
            "b", collectionOf(Pair.of(2L, "x"), Pair.of(1L, "z")));

    assertEquals(expected, actual);
  }

  @Test
  public void testGlobalToplist() {
    PCollection<String> data = MemPipeline.typedCollectionOf(strings(), "a", "a", "a", "b", "b", "c", "c", "c", "c");
    Map<String, Long> actual = TopList.globalToplist(data).materializeToMap();
    Map<String, Long> expected = ImmutableMap.of("c", 4L, "a", 3L, "b", 2L);
    assertEquals(expected, actual);
  }
}