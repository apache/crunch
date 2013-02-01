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

import static org.junit.Assert.assertNotNull;

import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class UnionGbkIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  MRPipeline pipeline;

  public static class FirstLetterKeyFn extends DoFn<String, Pair<String, String>> {
    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      if (input.length() > 0) {
        emitter.emit(Pair.of(input.substring(0, 1), input));
      }
    }
  }

  public static class ConcatGroupFn extends DoFn<Pair<String, Iterable<String>>, String> {
    @Override
    public void process(Pair<String, Iterable<String>> input, Emitter<String> emitter) {
      StringBuilder sb = new StringBuilder();
      for (String str : input.second()) {
        sb.append(str);
      }
      emitter.emit(sb.toString());
    }
  }
  
  @Before
  public void setUp() {
    pipeline = new MRPipeline(UnionGbkIT.class, tmpDir.getDefaultConfiguration());
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  @Test
  public void tableOfUnionGbk() throws Exception {
    PCollection<String> words = pipeline.readTextFile(
        tmpDir.copyResourceFileName("shakes.txt"));
    PCollection<String> lorum = pipeline.readTextFile(
        tmpDir.copyResourceFileName("maugham.txt"));
    lorum.materialize();

    @SuppressWarnings("unchecked")
    PCollection<String> union = words.union(lorum);

    PGroupedTable<String, String> groupedByFirstLetter =
        union.parallelDo("byFirstLetter", new FirstLetterKeyFn(),
            Avros.tableOf(Avros.strings(), Avros.strings()))
        .groupByKey();
    PCollection<String> concatted = groupedByFirstLetter
        .parallelDo("concat", new ConcatGroupFn(), Avros.strings());

    assertNotNull(concatted.materialize().iterator());
  }

  @Test
  public void unionOfTablesGbk() throws Exception {
    PCollection<String> words = pipeline.readTextFile(
        tmpDir.copyResourceFileName("shakes.txt"));
    PCollection<String> lorum = pipeline.readTextFile(
        tmpDir.copyResourceFileName("maugham.txt"));
    lorum.materialize();

    PTable<String, String> wordsByFirstLetter =
        words.parallelDo("byFirstLetter", new FirstLetterKeyFn(),
            Avros.tableOf(Avros.strings(), Avros.strings()));
    PTable<String, String> lorumByFirstLetter =
        lorum.parallelDo("byFirstLetter", new FirstLetterKeyFn(),
            Avros.tableOf(Avros.strings(), Avros.strings()));

    @SuppressWarnings("unchecked")
    PTable<String, String> union = wordsByFirstLetter.union(lorumByFirstLetter);

    PGroupedTable<String, String> groupedByFirstLetter = union.groupByKey();

    PCollection<String> concatted = groupedByFirstLetter.parallelDo("concat",
        new ConcatGroupFn(), Avros.strings());

    assertNotNull(concatted.materialize().iterator());
  }
}
