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

import java.io.IOException;

import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Collection of tests re-using the same PCollection in various unions.
 */
public class UnionFromSameSourceIT {

  private static final int NUM_ELEMENTS = 4;

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  private Pipeline pipeline;
  private PType<String> elementType = Writables.strings();
  private PTableType<String, String> tableType = Writables.tableOf(Writables.strings(),
    Writables.strings());

  @Before
  public void setUp() {
    pipeline = new MRPipeline(UnionFromSameSourceIT.class, tmpDir.getDefaultConfiguration());
  }

  @Test
  public void testUnion_SingleRead() throws IOException {
    PCollection<String> strings = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));
    PCollection<String> union = strings.union(strings.parallelDo(IdentityFn.<String> getInstance(),
      strings.getPType()));

    assertEquals(NUM_ELEMENTS * 2, getCount(union));
  }

  @Test
  public void testUnion_TwoReads() throws IOException {
    PCollection<String> stringsA = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));
    PCollection<String> stringsB = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));

    PCollection<String> union = stringsA.union(stringsB);

    assertEquals(NUM_ELEMENTS * 2, getCount(union));
  }

  @Test
  public void testDoubleUnion_EndingWithGBK() throws IOException {
    runDoubleUnionPipeline(true);
  }

  @Test
  public void testDoubleUnion_EndingWithoutGBK() throws IOException {
    runDoubleUnionPipeline(false);
  }

  private void runDoubleUnionPipeline(boolean endWithGBK) throws IOException {
    PCollection<String> strings = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));
    PTable<String, String> tableA = strings.parallelDo("to table A", new ToTableFn(), tableType);
    PTable<String, String> tableB = strings.parallelDo("to table B", new ToTableFn(), tableType);

    PGroupedTable<String, String> groupedTable = tableA.union(tableB).groupByKey();
    PCollection<String> ungrouped = groupedTable.parallelDo("ungroup before union",
      new FromGroupedTableFn(), elementType).union(
      strings.parallelDo("fake id", IdentityFn.<String> getInstance(), elementType));

    PTable<String, String> table = ungrouped.parallelDo("union back to table", new ToTableFn(),
      tableType);

    if (endWithGBK) {
      table = table.groupByKey().ungroup();
    }

    assertEquals(3 * NUM_ELEMENTS, getCount(table));
  }

  private int getCount(PCollection<?> pcollection) {
    int cnt = 0;
    for (Object v : pcollection.materialize()) {
      cnt++;
    }
    return cnt;
  }

  private static class ToTableFn extends MapFn<String, Pair<String, String>> {

    @Override
    public Pair<String, String> map(String input) {
      return Pair.of(input, input);
    }

  }

  private static class FromGroupedTableFn extends DoFn<Pair<String, Iterable<String>>, String> {

    @Override
    public void process(Pair<String, Iterable<String>> input, Emitter<String> emitter) {
      for (String value : input.second()) {
        emitter.emit(value);
      }
    }

  }

}
