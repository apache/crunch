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
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.lib.join.MapsideJoinStrategy;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.writable.Writables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparkMapsideJoinIT {
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

  private static class CapOrdersFn extends MapFn<String, String> {
    @Override
    public String map(String v) {
      return v.toUpperCase(Locale.ENGLISH);
    }
  }

  private static class ConcatValuesFn extends MapFn<Pair<String, String>, String> {
    @Override
    public String map(Pair<String, String> v) {
      return v.toString();
    }
  }

  @Rule
  public TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testMapsideJoin_RightSideIsEmpty() throws IOException {
    Pipeline pipeline = new SparkPipeline("local", "mapside");
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    PTable<Integer, String> filteredOrderTable = orderTable
        .parallelDo(FilterFns.<Pair<Integer, String>>REJECT_ALL(), orderTable.getPTableType());


    JoinStrategy<Integer, String, String> mapsideJoin = new MapsideJoinStrategy<Integer, String, String>();
    PTable<Integer, Pair<String, String>> joined = mapsideJoin.join(customerTable, filteredOrderTable, JoinType.INNER_JOIN);

    List<Pair<Integer, Pair<String, String>>> materializedJoin = Lists.newArrayList(joined.materialize());

    assertTrue(materializedJoin.isEmpty());
    pipeline.done();
  }

  @Test
  public void testMapsideJoin() throws IOException {
    runMapsideJoin(new SparkPipeline("local", "mapside"), false);
  }

  @Test
  public void testMapsideJoin_Materialized() throws IOException {
    runMapsideJoin(new SparkPipeline("local", "mapside"), true);
  }

  @Test
  public void testMapsideJoin_LeftOuterJoin() throws IOException {
    runMapsideLeftOuterJoin(new SparkPipeline("local", "mapside"), false);
  }

  @Test
  public void testMapsideJoin_LeftOuterJoin_Materialized() throws IOException {
    runMapsideLeftOuterJoin(new SparkPipeline("local", "mapside"), true);
  }

  private void runMapsideJoin(Pipeline pipeline, boolean materialize) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    JoinStrategy<Integer, String, String> mapsideJoin = new MapsideJoinStrategy<Integer, String, String>(materialize);
    PTable<Integer, String> custOrders = mapsideJoin.join(customerTable, orderTable, JoinType.INNER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined = mapsideJoin.join(custOrders, ORDER_TABLE, JoinType.INNER_JOIN);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("[John Doe,Corn flakes]", "CORN FLAKES")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(333, Pair.of("[Someone Else,Toilet brush]", "TOILET BRUSH")));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();

    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);
    assertEquals(expectedJoinResult, joinedResultList);
    pipeline.done();
  }

  private void runMapsideLeftOuterJoin(Pipeline pipeline, boolean materialize) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    JoinStrategy<Integer, String, String> mapsideJoin = new MapsideJoinStrategy<Integer, String, String>(materialize);
    PTable<Integer, String> custOrders = mapsideJoin.join(customerTable, orderTable, JoinType.LEFT_OUTER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined = mapsideJoin.join(custOrders, ORDER_TABLE, JoinType.LEFT_OUTER_JOIN);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("[John Doe,Corn flakes]", "CORN FLAKES")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(333, Pair.of("[Someone Else,Toilet brush]", "TOILET BRUSH")));
    expectedJoinResult.add(Pair.of(444, Pair.<String,String>of("[Has No Orders,null]", null)));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();

    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);
    assertEquals(expectedJoinResult, joinedResultList);
    pipeline.done();
  }

  private PTable<Integer, String> readTable(Pipeline pipeline, String filename) {
    try {
      return pipeline.readTextFile(tmpDir.copyResourceFileName(filename)).parallelDo("asTable",
          new LineSplitter(),
          Writables.tableOf(Writables.ints(), Writables.strings()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
