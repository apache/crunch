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
package org.apache.crunch.lib.join;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class OneToManyJoinTest {

  @Test
  public void testOneToMany() {
    PTable<Integer, String> left = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 
                                      1, "one", 2, "two");
    PTable<Integer, String> right = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 
                                      1, "1A", 1, "1B", 2, "2A", 2, "2B");

    PCollection<Pair<String, String>> joined = OneToManyJoin.oneToManyJoin(left, right, new StringJoinFn(),
        Avros.pairs(Avros.strings(), Avros.strings()));

    List<Pair<String, String>> expected = ImmutableList.of(Pair.of("one", "1A,1B"), Pair.of("two", "2A,2B"));

    assertEquals(expected, Lists.newArrayList(joined.materialize()));

  }
  
  @Test
  public void testOneToMany_UnmatchedOnRightSide() {
    PTable<Integer, String> left = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 1, "one", 2,
        "two");
    PTable<Integer, String> right = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 2, "2A", 2, "2B");

    PCollection<Pair<String, String>> joined = OneToManyJoin.oneToManyJoin(left, right, new StringJoinFn(),
        Avros.pairs(Avros.strings(), Avros.strings()));

    List<Pair<String, String>> expected = ImmutableList.of(Pair.of("two", "2A,2B"));

    assertEquals(expected, Lists.newArrayList(joined.materialize()));
  }
  
  @Test
  public void testOneToMany_UnmatchedLeftSide() {
    PTable<Integer, String> left = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 2, "two");
    PTable<Integer, String> right = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 1, "1A", 1,
        "1B", 2, "2A", 2, "2B");

    PCollection<Pair<String, String>> joined = OneToManyJoin.oneToManyJoin(left, right, new StringJoinFn(),
        Avros.pairs(Avros.strings(), Avros.strings()));

    List<Pair<String, String>> expected = ImmutableList.of(Pair.of("two", "2A,2B"));

    assertEquals(expected, Lists.newArrayList(joined.materialize()));
  }
  
  @Test
  public void testOneToMany_MultipleValuesForSameKeyOnLeft() {
    PTable<Integer, String> left = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 
                                      1, "one", 2, "two", 1, "oneExtra");
    PTable<Integer, String> right = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()), 
                                      1, "1A", 1, "1B", 2, "2A", 2, "2B");

    PCollection<Pair<String, String>> joined = OneToManyJoin.oneToManyJoin(left, right, new StringJoinFn(),
        Avros.pairs(Avros.strings(), Avros.strings()));

    List<Pair<String, String>> expected = ImmutableList.of(Pair.of("one", "1A,1B"), Pair.of("two", "2A,2B"));

    assertEquals(expected, Lists.newArrayList(joined.materialize()));

  }

  static class StringJoinFn extends MapFn<Pair<String, Iterable<String>>, Pair<String, String>> {

    @Override
    public Pair<String, String> map(Pair<String, Iterable<String>> input) {
      increment("counters", "inputcount");
      return Pair.of(input.first(), Joiner.on(',').join(input.second()));
    }

  }

}
