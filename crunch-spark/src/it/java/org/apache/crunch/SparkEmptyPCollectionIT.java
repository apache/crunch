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

import com.google.common.collect.Iterables;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SparkEmptyPCollectionIT {
  private static class SplitFn extends DoFn<String, Pair<String, Long>> {
    @Override
    public void process(String input, Emitter<Pair<String, Long>> emitter) {
      for (String word : input.split("\\s+")) {
        emitter.emit(Pair.of(word, 1L));
      }
    }
  }

  @Rule
  public TemporaryPath tempDir = new TemporaryPath();

  @Test
  public void testEmptyMR() throws Exception {
    Pipeline p = new SparkPipeline("local", "empty");
    assertTrue(Iterables.isEmpty(p.emptyPCollection(Writables.strings())
        .parallelDo(new SplitFn(), Writables.tableOf(Writables.strings(), Writables.longs()))
        .groupByKey()
        .combineValues(Aggregators.SUM_LONGS())
        .materialize()));
    p.done();
  }

  @Test
  public void testUnionWithEmptyMR() throws Exception {
    Pipeline p = new SparkPipeline("local", "empty");
    assertFalse(Iterables.isEmpty(p.emptyPCollection(Writables.strings())
        .parallelDo(new SplitFn(), Writables.tableOf(Writables.strings(), Writables.longs()))
        .union(
            p.read(From.textFile(tempDir.copyResourceFileName("shakes.txt")))
                .parallelDo(new SplitFn(), Writables.tableOf(Writables.strings(), Writables.longs())))
        .groupByKey()
        .combineValues(Aggregators.SUM_LONGS())
        .materialize()));
    p.done();
  }

  @Test
  public void testUnionTableWithEmptyMR() throws Exception {
    Pipeline p = new SparkPipeline("local", "empty");
    assertFalse(Iterables.isEmpty(p.emptyPTable(Writables.tableOf(Writables.strings(), Writables.longs()))
        .union(
            p.read(From.textFile(tempDir.copyResourceFileName("shakes.txt")))
                .parallelDo(new SplitFn(), Writables.tableOf(Writables.strings(), Writables.longs())))
        .groupByKey()
        .combineValues(Aggregators.SUM_LONGS())
        .materialize()));
    p.done();
  }
}
