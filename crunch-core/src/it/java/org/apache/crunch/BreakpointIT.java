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
package org.apache.crunch;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BreakpointIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testNoBreakpoint() throws Exception {
    run(new MRPipeline(BreakpointIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("shakes.txt"),
        tmpDir.getFileName("out1"),
        tmpDir.getFileName("out2"),
        false);
  }

  @Test
  public void testBreakpoint() throws Exception {
    run(new MRPipeline(BreakpointIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("shakes.txt"),
        tmpDir.getFileName("out1"),
        tmpDir.getFileName("out2"),
        true);
  }

  public static void run(MRPipeline pipeline, String input, String out1, String out2, boolean breakpoint)
      throws Exception {

    // Read a line from a file to get a PCollection.
    PCollection<String> pCol1 = pipeline.read(From.textFile(input));

    // Create a PTable from PCollection
    PTable<String, Integer> pTable1 = pCol1.parallelDo(new DoFn<String, Pair<String, Integer>>() {
      @Override
      public void process(final String s, final Emitter<Pair<String, Integer>> emitter) {
        for (int i = 0; i < 10; i++) {
          emitter.emit(new Pair<String, Integer>(s, i));
        }
      }
    }, Writables.tableOf(Writables.strings(), Writables.ints()));

    // Do a groupByKey
    PGroupedTable<String, Integer> pGrpTable1 = pTable1.groupByKey();

    // Select from PGroupedTable
    PTable<String, Integer> selectFromPTable1 = pGrpTable1.parallelDo(
        new DoFn<Pair<String, Iterable<Integer>>, Pair<String, Integer>>() {
          @Override
          public void process(final Pair<String, Iterable<Integer>> input,
                              final Emitter<Pair<String, Integer>> emitter) {
            emitter.emit(new Pair<String, Integer>(input.first(), input.second().iterator().next()));
          }
        }, Writables.tableOf(Writables.strings(), Writables.ints()));

    // Process selectFromPTable1 once
    final PTable<String, String> pTable2 = selectFromPTable1.parallelDo(new DoFn<Pair<String, Integer>, Pair<String, String>>() {
      @Override
      public void process(final Pair<String, Integer> input, final Emitter<Pair<String, String>> emitter) {
        final Integer newInt = input.second() + 5;
        increment("job", "table2");
        emitter.emit(new Pair<String, String>(newInt.toString(), input.first()));
      }
    }, Writables.tableOf(Writables.strings(), Writables.strings()));

    // Process selectFromPTable1 once more
    PTable<String, String> pTable3 = selectFromPTable1.parallelDo(new DoFn<Pair<String, Integer>, Pair<String, String>>() {
      @Override
      public void process(final Pair<String, Integer> input, final Emitter<Pair<String, String>> emitter) {
        final Integer newInt = input.second() + 10;
        increment("job", "table3");
        emitter.emit(new Pair<String, String>(newInt.toString(), input.first()));
      }
    }, Writables.tableOf(Writables.strings(), Writables.strings()));

    // Union pTable2 and pTable3 and set a breakpoint
    PTable<String, String> pTable4 = pTable2.union(pTable3);
    if (breakpoint) {
      pTable4.materialize();
    }

    // Write keys
    pTable4.keys().write(To.textFile(out1));

    // Group values
    final PGroupedTable<String, String> pGrpTable3 = pTable4.groupByKey();

    // Write values
    pGrpTable3.ungroup().write(To.textFile(out2));

    MRExecutor exec = pipeline.plan();
    // Count the number of map processing steps in this pipeline
    int mapsCount = 0;
    for (String line : exec.getPlanDotFile().split("\n")) {
      if (line.contains(" subgraph ") && line.contains("-map\" {")) {
        mapsCount++;
      }
    }
    assertEquals(breakpoint ? 1 : 2, mapsCount);
  }
}
