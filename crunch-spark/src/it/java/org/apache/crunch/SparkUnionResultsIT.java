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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SparkUnionResultsIT extends CrunchTestSupport implements Serializable {

  static class StringLengthMapFn extends MapFn<String, Pair<String, Long>> {
    @Override
    public Pair<String, Long> map(String input) {
      increment("my", "counter");
      return new Pair<String, Long>(input, 10L);
    }
  }


  /**
   * Tests combining a GBK output with a map-only job output into a single
   * unioned collection.
   */
  @Test
  public void testUnionOfGroupedOutputAndNonGroupedOutput() throws IOException {
    String inputPath = tempDir.copyResourceFileName("set1.txt");
    String inputPath2 = tempDir.copyResourceFileName("set2.txt");

    Pipeline pipeline = new SparkPipeline("local", "unionresults");

    PCollection<String> set1Lines = pipeline.read(At.textFile(inputPath, Writables.strings()));
    PCollection<Pair<String, Long>> set1Lengths = set1Lines.parallelDo(new StringLengthMapFn(),
        Writables.pairs(Writables.strings(), Writables.longs()));
    PCollection<Pair<String, Long>> set2Counts = pipeline.read(At.textFile(inputPath2, Writables.strings())).count();

    PCollection<Pair<String, Long>> union = set1Lengths.union(set2Counts);

    Set<Pair<String, Long>> unionValues = Sets.newHashSet(union.materialize());
    assertEquals(7, unionValues.size());

    Set<Pair<String, Long>> expectedPairs = Sets.newHashSet();
    expectedPairs.add(Pair.of("b", 10L));
    expectedPairs.add(Pair.of("c", 10L));
    expectedPairs.add(Pair.of("a", 10L));
    expectedPairs.add(Pair.of("e", 10L));
    expectedPairs.add(Pair.of("a", 1L));
    expectedPairs.add(Pair.of("c", 1L));
    expectedPairs.add(Pair.of("d", 1L));

    assertEquals(expectedPairs, unionValues);

    pipeline.done();
  }

  @Test
  public void testMultiGroupBy() throws Exception {
    String inputPath = tempDir.copyResourceFileName("set1.txt");
    String inputPath2 = tempDir.copyResourceFileName("set2.txt");
    String output = tempDir.getFileName("output");

    Pipeline pipeline = new SparkPipeline("local", "multigroupby");

    PCollection<String> set1Lines = pipeline.read(At.textFile(inputPath, Writables.strings()));
    PCollection<Pair<String, Long>> set1Lengths = set1Lines.parallelDo(new StringLengthMapFn(),
        Writables.pairs(Writables.strings(), Writables.longs()));
    PTable<String, Long> set2Counts = pipeline.read(At.textFile(inputPath2, Writables.strings())).count();
    PTables.asPTable(set2Counts.union(set1Lengths)).groupByKey().ungroup()
        .write(At.sequenceFile(output, Writables.strings(), Writables.longs()));
    PipelineResult res = pipeline.done();
    assertEquals(4, res.getStageResults().get(0).getCounterValue("my", "counter"));
  }

  @Test
  public void testMultiWrite() throws Exception {
    String inputPath = tempDir.copyResourceFileName("set1.txt");
    String inputPath2 = tempDir.copyResourceFileName("set2.txt");
    String output = tempDir.getFileName("output");

    Pipeline pipeline = new SparkPipeline("local", "multiwrite");

    PCollection<String> set1Lines = pipeline.read(At.textFile(inputPath, Writables.strings()));
    PTable<String, Long> set1Lengths = set1Lines.parallelDo(new StringLengthMapFn(),
        Writables.tableOf(Writables.strings(), Writables.longs()));
    PTable<String, Long> set2Counts = pipeline.read(At.textFile(inputPath2, Writables.strings())).count();

    TableSourceTarget<String, Long> inter = At.sequenceFile(output, Writables.strings(), Writables.longs());
    set1Lengths.write(inter);
    set2Counts.write(inter, Target.WriteMode.APPEND);

    pipeline.run();

    PTable<String, Long> in = pipeline.read(inter);
    Set<Pair<String, Long>> values = Sets.newHashSet(in.materialize());
    assertEquals(7, values.size());

    Set<Pair<String, Long>> expectedPairs = Sets.newHashSet();
    expectedPairs.add(Pair.of("b", 10L));
    expectedPairs.add(Pair.of("c", 10L));
    expectedPairs.add(Pair.of("a", 10L));
    expectedPairs.add(Pair.of("e", 10L));
    expectedPairs.add(Pair.of("a", 1L));
    expectedPairs.add(Pair.of("c", 1L));
    expectedPairs.add(Pair.of("d", 1L));

    assertEquals(expectedPairs, values);

    pipeline.done();
  }
}
