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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Join;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

public class Breakpoint2IT {
  private static final class PTableTransform extends DoFn<String, Pair<String, Integer>> {
    @Override
    public void process(final String s, final Emitter<Pair<String, Integer>> emitter) {
      for (int i = 0; i < 10; i++) {
        emitter.emit(new Pair<String, Integer>(s, i));
      }
    }
  }

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testNoBreakpoint() throws Exception {
    run(new MRPipeline(Breakpoint2IT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("letters.txt"),
        tmpDir.copyResourceFileName("urls.txt"),
        tmpDir.copyResourceFileName("docs.txt"),
        tmpDir.getFileName("out1"),
        tmpDir.getFileName("out2"),
        false);
  }

  @Test
  public void testBreakpoint() throws Exception {
    run(new MRPipeline(Breakpoint2IT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("letters.txt"),
        tmpDir.copyResourceFileName("urls.txt"),
        tmpDir.copyResourceFileName("docs.txt"),
        tmpDir.getFileName("out1"),
        tmpDir.getFileName("out2"),
        true);
  }

  public static void run(MRPipeline pipeline, String input1, String input2, String input3,
                         String out1, String out2, boolean breakpoint) throws Exception {
    // Read a line from a file to get a PCollection.
    PCollection<String> pCol1 = pipeline.read(From.textFile(input1));
    PCollection<String> pCol2 = pipeline.read(From.textFile(input2));
    PCollection<String> pCol3 = pipeline.read(From.textFile(input3));

    // Create PTables from the PCollections
    PTable<String, Integer> pTable1 = pCol1.parallelDo("Transform pCol1 to PTable", new PTableTransform(),
        Writables.tableOf(Writables.strings(), Writables.ints()));
    if (breakpoint) {
      pTable1.materialize();
    }

    PTable<String, Integer> pTable2 = pCol2.parallelDo("Transform pCol2 to PTable", new PTableTransform(),
        Writables.tableOf(Writables.strings(), Writables.ints()));
    PTable<String, Integer> pTable3 = pCol3.parallelDo("Transform pCol3 to PTable", new PTableTransform(),
        Writables.tableOf(Writables.strings(), Writables.ints()));

    // Perform joins to pTable1
    PTable<String, Pair<Integer, Integer>> join1 = Join.leftJoin(pTable1, pTable2);
    PTable<String, Pair<Integer, Integer>> join2 = Join.rightJoin(pTable1, pTable3);

    // Write joins
    join1.keys().write(To.textFile(out1));
    join2.keys().write(To.textFile(out2));

    MRPipelineExecution exec = pipeline.runAsync();
    int fnCount = 0;
    for (String line : exec.getPlanDotFile().split("\n")) {
      if (line.contains("label=\"Transform pCol1 to PTable\"")) {
        fnCount++;
      }
    }
    assertEquals(breakpoint ? 1 : 2, fnCount);
    exec.waitUntilDone();
  }
}
