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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.fn.MapValuesFn;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MapsideJoinIT {
  
  private static String saveTempDir;
  
  @BeforeClass
  public static void setUpClass(){
    
    // Ensure a consistent temporary directory for use of the DistributedCache.
    
    // The DistributedCache technically isn't supported when running in local mode, and the default
    // temporary directiory "/tmp" is used as its location. This typically only causes an issue when 
    // running integration tests on Mac OS X, as OS X doesn't use "/tmp" as it's default temporary
    // directory. The following call ensures that "/tmp" is used as the temporary directory on all platforms.
    saveTempDir = System.setProperty("java.io.tmpdir", "/tmp");
  }
  
  @AfterClass
  public static void tearDownClass(){
    System.setProperty("java.io.tmpdir", saveTempDir);
  }

  private static class LineSplitter extends MapFn<String, Pair<Integer, String>> {
    @Override
    public Pair<Integer, String> map(String input) {
      String[] fields = input.split("\\|");
      return Pair.of(Integer.parseInt(fields[0]), fields[1]);
    }
  }

  private static class CapOrdersFn extends MapValuesFn<Integer, String, String> {
    @Override
    public String map(String v) {
      return v.toUpperCase();
    }
  }
  
  private static class ConcatValuesFn extends MapValuesFn<Integer, Pair<String, String>, String> {
    @Override
    public String map(Pair<String, String> v) {
      return v.toString();
    }
  }
  
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMapSideJoin_MemPipeline() {
    runMapsideJoin(MemPipeline.getInstance(), true);
  }

  @Test
  public void testMapsideJoin_RightSideIsEmpty() throws IOException {
    MRPipeline pipeline = new MRPipeline(MapsideJoinIT.class, tmpDir.getDefaultConfiguration());
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    PTable<Integer, String> filteredOrderTable = orderTable
        .parallelDo(FilterFns.<Pair<Integer, String>>REJECT_ALL(), orderTable.getPTableType());

    PTable<Integer, Pair<String, String>> joined = MapsideJoin.join(customerTable, filteredOrderTable);

    List<Pair<Integer, Pair<String, String>>> materializedJoin = Lists.newArrayList(joined.materialize());

    assertTrue(materializedJoin.isEmpty());
  }

  @Test
  public void testMapsideJoin() throws IOException {
    runMapsideJoin(new MRPipeline(MapsideJoinIT.class, tmpDir.getDefaultConfiguration()), false);
  }

  private void runMapsideJoin(Pipeline pipeline, boolean inMemory) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");
    
    PTable<Integer, String> custOrders = MapsideJoin.join(customerTable, orderTable)
        .parallelDo("concat", new ConcatValuesFn(), Writables.tableOf(Writables.ints(), Writables.strings()));

    PTable<Integer, String> ORDER_TABLE = orderTable.parallelDo(new CapOrdersFn(), orderTable.getPTableType());
    
    PTable<Integer, Pair<String, String>> joined = MapsideJoin.join(custOrders, ORDER_TABLE);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("[John Doe,Corn flakes]", "CORN FLAKES")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(333, Pair.of("[Someone Else,Toilet brush]", "TOILET BRUSH")));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();
    
    PipelineResult res = pipeline.run();
    if (!inMemory) {
      assertEquals(2, res.getStageResults().size());
    }
     
    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);

    assertEquals(expectedJoinResult, joinedResultList);
  }

  private PTable<Integer, String> readTable(Pipeline pipeline, String filename) {
    try {
      return pipeline.readTextFile(tmpDir.copyResourceFileName(filename)).parallelDo("asTable", new LineSplitter(),
          Writables.tableOf(Writables.ints(), Writables.strings()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
