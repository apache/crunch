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
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import com.google.common.collect.Lists;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class MapsideJoinStrategyIT {
  
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
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMapSideJoin_MemPipeline() {
    runMapsideJoin(MemPipeline.getInstance(), true, false, MapsideJoinStrategy.<Integer,String,String>create(false));
  }

  @Test
  public void testLegacyMapSideJoin_MemPipeline() {
    runLegacyMapsideJoin(MemPipeline.getInstance(), true, false, new MapsideJoinStrategy<Integer, String, String>(false));
  }

  @Test
  public void testMapSideJoin_MemPipeline_Materialized() {
    runMapsideJoin(MemPipeline.getInstance(), true, true, MapsideJoinStrategy.<Integer,String,String>create(true));
  }

  @Test
  public void testLegacyMapSideJoin_MemPipeline_Materialized() {
    runLegacyMapsideJoin(MemPipeline.getInstance(), true, true, new MapsideJoinStrategy<Integer, String, String>(true));
  }
  
  @Test
  public void testMapSideJoinRightOuterJoin_MemPipeline() {
    runMapsideRightOuterJoin(MemPipeline.getInstance(), true, false,
                             MapsideJoinStrategy.<Integer, String, String>create(false));
  }

  @Test
  public void testLegacyMapSideJoinLeftOuterJoin_MemPipeline() {
    runLegacyMapsideLeftOuterJoin(MemPipeline.getInstance(), true, false, new MapsideJoinStrategy<Integer, String, String>(false));
  }

  @Test
  public void testMapSideJoinRightOuterJoin_MemPipeline_Materialized() {
    runMapsideRightOuterJoin(MemPipeline.getInstance(), true, true,
                             MapsideJoinStrategy.<Integer, String, String>create(true));
  }

  @Test
  public void testLegacyMapSideJoinLeftOuterJoin_MemPipeline_Materialized() {
    runLegacyMapsideLeftOuterJoin(MemPipeline.getInstance(), true, true, new MapsideJoinStrategy<Integer, String, String>(true));
  }

  @Test
  public void testMapsideJoin_RightSideIsEmpty() throws IOException {
    MRPipeline pipeline = new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration());
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    PTable<Integer, String> filteredOrderTable = orderTable
        .parallelDo(FilterFns.<Pair<Integer, String>>REJECT_ALL(), orderTable.getPTableType());

    
    JoinStrategy<Integer, String, String> mapsideJoin = new MapsideJoinStrategy<Integer, String, String>();
    PTable<Integer, Pair<String, String>> joined = mapsideJoin.join(customerTable, filteredOrderTable, JoinType.INNER_JOIN);

    List<Pair<Integer, Pair<String, String>>> materializedJoin = Lists.newArrayList(joined.materialize());

    assertTrue(materializedJoin.isEmpty());
  }

  @Test
  public void testLegacyMapsideJoin_LeftSideIsEmpty() throws IOException {
    MRPipeline pipeline = new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration());
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    readTable(pipeline, "orders.txt");

    PTable<Integer, String> filteredCustomerTable = customerTable
        .parallelDo(FilterFns.<Pair<Integer, String>>REJECT_ALL(), customerTable.getPTableType());


    JoinStrategy<Integer, String, String> mapsideJoin = new MapsideJoinStrategy<Integer, String, String>();
    PTable<Integer, Pair<String, String>> joined = mapsideJoin.join(customerTable, filteredCustomerTable,
                                                                    JoinType.INNER_JOIN);

    List<Pair<Integer, Pair<String, String>>> materializedJoin = Lists.newArrayList(joined.materialize());

    assertTrue(materializedJoin.isEmpty());
  }

  @Test
  public void testMapsideJoin() throws IOException {
    runMapsideJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                   false, false, MapsideJoinStrategy.<Integer, String, String>create(false));
  }

  @Test
  public void testLegacyMapsideJoin() throws IOException {
    runLegacyMapsideJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                   false, false, new MapsideJoinStrategy<Integer, String, String>(false));
  }

  @Test
  public void testMapsideJoin_Materialized() throws IOException {
    runMapsideJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                   false, true, MapsideJoinStrategy.<Integer, String, String>create(true));
  }

  @Test
  public void testLegacyMapsideJoin_Materialized() throws IOException {
    runLegacyMapsideJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                   false, true, new MapsideJoinStrategy<Integer, String, String>(true));
  }

  @Test
  public void testMapsideJoin_RightOuterJoin() throws IOException {
    runMapsideRightOuterJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                             false, false, MapsideJoinStrategy.<Integer, String, String>create(false));
  }

  @Test
  public void testLegacyMapsideJoin_LeftOuterJoin() throws IOException {
    runLegacyMapsideLeftOuterJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                                  false, false,
                                  new MapsideJoinStrategy<Integer, String, String>(false));
  }

  @Test
  public void testMapsideJoin_RightOuterJoin_Materialized() throws IOException {
    runMapsideRightOuterJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                             false, true, MapsideJoinStrategy.<Integer, String, String>create(true));
  }

  @Test
  public void testLegacyMapsideJoin_LeftOuterJoin_Materialized() throws IOException {
    runLegacyMapsideLeftOuterJoin(new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration()),
                                  false, true,
                                  new MapsideJoinStrategy<Integer, String, String>(true));
  }

  @Test
  public void testMapSideJoinWithImmutableBytesWritable() throws IOException, InterruptedException {
    //Write out input files
    FileSystem fs = FileSystem.get(tmpDir.getDefaultConfiguration());
    Path path1 = tmpDir.getPath("input1.txt");
    Path path2 = tmpDir.getPath("input2.txt");

    OutputStream out1 = fs.create(path1, true);
    OutputStream out2 = fs.create(path2, true);

    for(int i = 0; i < 4; i++){
      byte[] value = ("value" + i + "\n").getBytes(Charset.forName("UTF-8"));
      out1.write(value);
      out2.write(value);
    }

    out1.flush();
    out1.close();
    out2.flush();
    out2.close();

    final MRPipeline pipeline = new MRPipeline(MapsideJoinStrategyIT.class, tmpDir.getDefaultConfiguration());

    final PCollection<String> values1 = pipeline.readTextFile(path1.toString());
    final PCollection<String> values2 = pipeline.readTextFile(path2.toString());

    final PTable<Text, Text> convertedValues1 = convertStringToText(values1);
    final PTable<Text, Text> convertedValues2 = convertStringToText(values2);

    // for map side join
    final MapsideJoinStrategy<Text, Text, Text> mapSideJoinStrategy = MapsideJoinStrategy.<Text, Text, Text>create();

    final PTable<Text, Pair<Text, Text>> updatedJoinedRows = mapSideJoinStrategy.join(convertedValues1, convertedValues2, JoinType.INNER_JOIN);
    pipeline.run();

    // Join should have 2 results
    // Join should have contentBytes1 and contentBytes2
    assertEquals(4, updatedJoinedRows.materializeToMap().size());
  }

  /**
   * The method is used to convert string to entity key
   */
  public static PTable<Text, Text> convertStringToText(final PCollection<String> entityKeysStringPCollection) {
    return entityKeysStringPCollection.parallelDo(new DoFn<String, Pair<Text, Text>>() {

      @Override
      public void process(final String input, final Emitter<Pair<Text, Text>> emitter) {
        emitter.emit(new Pair<Text, Text>(new Text(input), new Text(input)));
      }
    }, Writables.tableOf(Writables.writables(Text.class), Writables.writables(Text.class)));
  }


  private void runMapsideJoin(Pipeline pipeline, boolean inMemory, boolean materialize,
                              MapsideJoinStrategy<Integer,String, String> joinStrategy) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");
    
    PTable<Integer, String> custOrders = joinStrategy.join(orderTable, customerTable, JoinType.INNER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined = joinStrategy.join(ORDER_TABLE, custOrders, JoinType.INNER_JOIN);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("CORN FLAKES", "[Corn flakes,John Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PAPER", "[Toilet paper,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PAPER", "[Toilet plunger,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PLUNGER", "[Toilet paper,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PLUNGER", "[Toilet plunger,Jane Doe]")));
    expectedJoinResult.add(Pair.of(333, Pair.of("TOILET BRUSH", "[Toilet brush,Someone Else]")));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();
    
    PipelineResult res = pipeline.run();
    if (!inMemory) {
      assertEquals(materialize ? 2 : 1, res.getStageResults().size());
    }
     
    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);

    assertEquals(expectedJoinResult, joinedResultList);
  }

  private void runLegacyMapsideJoin(Pipeline pipeline, boolean inMemory, boolean materialize,
                                    MapsideJoinStrategy<Integer, String, String> mapsideJoinStrategy) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    PTable<Integer, String> custOrders = mapsideJoinStrategy.join(customerTable, orderTable, JoinType.INNER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined = mapsideJoinStrategy.join(custOrders, ORDER_TABLE, JoinType.INNER_JOIN);

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
      assertEquals(materialize ? 2 : 1, res.getStageResults().size());
    }

    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);

    assertEquals(expectedJoinResult, joinedResultList);
  }
  
  private void runMapsideRightOuterJoin(Pipeline pipeline, boolean inMemory, boolean materialize,
                                        MapsideJoinStrategy<Integer, String, String> mapsideJoinStrategy) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");
    
    PTable<Integer, String> custOrders = mapsideJoinStrategy.join(orderTable, customerTable, JoinType.RIGHT_OUTER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined = mapsideJoinStrategy.join(ORDER_TABLE, custOrders,
                                                                     JoinType.RIGHT_OUTER_JOIN);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("CORN FLAKES", "[Corn flakes,John Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PAPER", "[Toilet paper,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PAPER", "[Toilet plunger,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PLUNGER", "[Toilet paper,Jane Doe]")));
    expectedJoinResult.add(Pair.of(222, Pair.of("TOILET PLUNGER", "[Toilet plunger,Jane Doe]")));
    expectedJoinResult.add(Pair.of(333, Pair.of("TOILET BRUSH", "[Toilet brush,Someone Else]")));
    expectedJoinResult.add(Pair.of(444, Pair.<String,String>of(null, "[null,Has No Orders]")));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();
    
    PipelineResult res = pipeline.run();
    if (!inMemory) {
      assertEquals(materialize ? 2 : 1, res.getStageResults().size());
    }
     
    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);

    assertEquals(expectedJoinResult, joinedResultList);
  }

  private void runLegacyMapsideLeftOuterJoin(Pipeline pipeline, boolean inMemory, boolean materialize,
                                             MapsideJoinStrategy<Integer, String, String> legacyMapsideJoinStrategy) {
    PTable<Integer, String> customerTable = readTable(pipeline, "customers.txt");
    PTable<Integer, String> orderTable = readTable(pipeline, "orders.txt");

    PTable<Integer, String> custOrders = legacyMapsideJoinStrategy.join(customerTable, orderTable,
                                                                        JoinType.LEFT_OUTER_JOIN)
        .mapValues("concat", new ConcatValuesFn(), Writables.strings());

    PTable<Integer, String> ORDER_TABLE = orderTable.mapValues(new CapOrdersFn(), orderTable.getValueType());
    PTable<Integer, Pair<String, String>> joined =
        legacyMapsideJoinStrategy.join(custOrders, ORDER_TABLE, JoinType.LEFT_OUTER_JOIN);

    List<Pair<Integer, Pair<String, String>>> expectedJoinResult = Lists.newArrayList();
    expectedJoinResult.add(Pair.of(111, Pair.of("[John Doe,Corn flakes]", "CORN FLAKES")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet paper]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PAPER")));
    expectedJoinResult.add(Pair.of(222, Pair.of("[Jane Doe,Toilet plunger]", "TOILET PLUNGER")));
    expectedJoinResult.add(Pair.of(333, Pair.of("[Someone Else,Toilet brush]", "TOILET BRUSH")));
    expectedJoinResult.add(Pair.of(444, Pair.<String,String>of("[Has No Orders,null]", null)));
    Iterable<Pair<Integer, Pair<String, String>>> iter = joined.materialize();

    PipelineResult res = pipeline.run();
    if (!inMemory) {
      assertEquals(materialize ? 2 : 1, res.getStageResults().size());
    }

    List<Pair<Integer, Pair<String, String>>> joinedResultList = Lists.newArrayList(iter);
    Collections.sort(joinedResultList);

    assertEquals(expectedJoinResult, joinedResultList);
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
