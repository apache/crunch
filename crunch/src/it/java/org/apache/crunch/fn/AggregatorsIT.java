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
package org.apache.crunch.fn;

import static org.apache.crunch.fn.Aggregators.SUM_INTS;
import static org.apache.crunch.fn.Aggregators.pairAggregator;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.pairs;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.test.Tests;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class AggregatorsIT {
  private Pipeline pipeline;

  @Parameters
  public static Collection<Object[]> params() {
    return Tests.pipelinesParams(AggregatorsIT.class);
  }

  public AggregatorsIT(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  @Test
  public void testPairAggregator() {
    PCollection<String> lines = pipeline.readTextFile(Tests.pathTo(this, "ints.txt"));

    PTable<String, Pair<Integer, Integer>> table = lines.parallelDo(new SplitLine(),
        tableOf(strings(), pairs(ints(), ints())));

    PTable<String, Pair<Integer, Integer>> combinedTable = table.groupByKey().combineValues(
        pairAggregator(SUM_INTS(), SUM_INTS()));

    Map<String, Pair<Integer, Integer>> result = combinedTable.asMap().getValue();

    assertThat(result.size(), is(2));
    assertThat(result.get("a"), is(Pair.of(9,  12)));
    assertThat(result.get("b"), is(Pair.of(11,  13)));
  }

  private static final class SplitLine extends MapFn<String, Pair<String, Pair<Integer, Integer>>> {
    @Override
    public Pair<String, Pair<Integer, Integer>> map(String input) {
      String[] split = input.split("\t");
      return Pair.of(split[0],
          Pair.of(Integer.parseInt(split[1]), Integer.parseInt(split[2])));
    }
  }

}
