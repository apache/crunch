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
package org.apache.crunch.lib;

import static org.apache.crunch.lib.Sort.ColumnOrder.by;
import static org.apache.crunch.lib.Sort.Order.ASCENDING;
import static org.apache.crunch.lib.Sort.Order.DESCENDING;
import static org.apache.crunch.test.StringWrapper.wrap;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SortIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testWritableSortAsc() throws Exception {
    runSingle(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), Order.ASCENDING,
        "A\tand this text as well");
  }

  @Test
  public void testWritableSortDesc() throws Exception {
    runSingle(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), Order.DESCENDING,
        "B\tthis doc has some text");
  }

  @Test
  public void testWritableSortAscDesc() throws Exception {
    runPair(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING), "A",
        "this doc has this text");
  }

  @Test
  public void testWritableSortSecondDescFirstAsc() throws Exception {
    runPair(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), by(2, DESCENDING), by(1, ASCENDING), "A",
        "this doc has this text");
  }

  @Test
  public void testWritableSortTripleAscDescAsc() throws Exception {
    runTriple(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING),
        by(3, ASCENDING), "A", "this", "doc");
  }

  @Test
  public void testWritableSortQuadAscDescAscDesc() throws Exception {
    runQuad(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING),
        by(3, ASCENDING), by(4, DESCENDING), "A", "this", "doc", "has");
  }

  @Test
  public void testWritableSortTupleNAscDesc() throws Exception {
    runTupleN(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(),
        new ColumnOrder[] { by(1, ASCENDING), by(2, DESCENDING) }, new String[] { "A", "this doc has this text" });
  }

  @Test
  public void testWritableSortTable() throws Exception {
    runTable(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), "A");
  }

  @Test
  public void testAvroSortAsc() throws Exception {
    runSingle(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), Order.ASCENDING, "A\tand this text as well");
  }

  @Test
  public void testAvroSortDesc() throws Exception {
    runSingle(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), Order.DESCENDING, "B\tthis doc has some text");
  }

  @Test
  public void testAvroSortPairAscDesc() throws Exception {
    runPair(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING), "A",
        "this doc has this text");
  }

  @Test
  public void testAvroSortPairSecondDescFirstAsc() throws Exception {
    runPair(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), by(2, DESCENDING), by(1, ASCENDING), "A",
        "this doc has this text");
  }

  @Test
  public void testAvroSortTripleAscDescAsc() throws Exception {
    runTriple(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING),
        by(3, ASCENDING), "A", "this", "doc");
  }

  @Test
  public void testAvroSortQuadAscDescAscDesc() throws Exception {
    runQuad(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), by(1, ASCENDING), by(2, DESCENDING),
        by(3, ASCENDING), by(4, DESCENDING), "A", "this", "doc", "has");
  }

  @Test
  public void testAvroSortTupleNAscDesc() throws Exception {
    runTupleN(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(),
        new ColumnOrder[] { by(1, ASCENDING), by(2, DESCENDING) }, new String[] { "A", "this doc has this text" });
  }

  @Test
  public void testAvroReflectSortPair() throws IOException {
    Pipeline pipeline = new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration());
    pipeline.enableDebug();
    String rsrc = tmpDir.copyResourceFileName("set2.txt");
    PCollection<Pair<String, StringWrapper>> in = pipeline.readTextFile(rsrc)
        .parallelDo(new MapFn<String, Pair<String, StringWrapper>>() {

          @Override
          public Pair<String, StringWrapper> map(String input) {
            return Pair.of(input, wrap(input));
          }
        }, Avros.pairs(Avros.strings(), Avros.reflects(StringWrapper.class)));
    PCollection<Pair<String, StringWrapper>> sorted = Sort.sort(in, Order.ASCENDING);
    
    List<Pair<String, StringWrapper>> expected = Lists.newArrayList();
    expected.add(Pair.of("a", wrap("a")));
    expected.add(Pair.of("c", wrap("c")));
    expected.add(Pair.of("d", wrap("d")));

    assertEquals(expected, Lists.newArrayList(sorted.materialize()));
  }

  @Test
  public void testAvroReflectSortTable() throws IOException {
    Pipeline pipeline = new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration());
    PTable<String, StringWrapper> unsorted = pipeline.readTextFile(tmpDir.copyResourceFileName("set2.txt")).parallelDo(
        new MapFn<String, Pair<String, StringWrapper>>() {

          @Override
          public Pair<String, StringWrapper> map(String input) {
            return Pair.of(input, wrap(input));
          }
        }, Avros.tableOf(Avros.strings(), Avros.reflects(StringWrapper.class)));

    PTable<String, StringWrapper> sorted = Sort.sort(unsorted);

    List<Pair<String, StringWrapper>> expected = Lists.newArrayList();
    expected.add(Pair.of("a", wrap("a")));
    expected.add(Pair.of("c", wrap("c")));
    expected.add(Pair.of("d", wrap("d")));

    assertEquals(expected, Lists.newArrayList(sorted.materialize()));
  }

  @Test
  public void testAvroSortTable() throws Exception {
    runTable(new MRPipeline(SortIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance(), "A");
  }

  private void runSingle(Pipeline pipeline, PTypeFamily typeFamily, Order order, String firstLine) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    // following turns the input from Writables to required type family
    PCollection<String> input2 = input.parallelDo(new DoFn<String, String>() {
      @Override
      public void process(String input, Emitter<String> emitter) {
        emitter.emit(input);
      }
    }, typeFamily.strings());
    PCollection<String> sorted = Sort.sort(input2, order);
    Iterable<String> lines = sorted.materialize();

    assertEquals(firstLine, lines.iterator().next());
    pipeline.done(); // TODO: finally
  }

  private void runPair(Pipeline pipeline, PTypeFamily typeFamily, ColumnOrder first, ColumnOrder second,
      String firstField, String secondField) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    PTable<String, String> kv = input.parallelDo(new DoFn<String, Pair<String, String>>() {
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        String[] split = input.split("[\t]+");
        emitter.emit(Pair.of(split[0], split[1]));
      }
    }, typeFamily.tableOf(typeFamily.strings(), typeFamily.strings()));
    PCollection<Pair<String, String>> sorted = Sort.sortPairs(kv, first, second);
    List<Pair<String, String>> lines = Lists.newArrayList(sorted.materialize());
    Pair<String, String> l = lines.iterator().next();
    assertEquals(firstField, l.first());
    assertEquals(secondField, l.second());
    pipeline.done();
  }

  private void runTriple(Pipeline pipeline, PTypeFamily typeFamily, ColumnOrder first, ColumnOrder second,
      ColumnOrder third, String firstField, String secondField, String thirdField) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    PCollection<Tuple3<String, String, String>> kv = input.parallelDo(
        new DoFn<String, Tuple3<String, String, String>>() {
          @Override
          public void process(String input, Emitter<Tuple3<String, String, String>> emitter) {
            String[] split = input.split("[\t ]+");
            int len = split.length;
            emitter.emit(Tuple3.of(split[0], split[1 % len], split[2 % len]));
          }
        }, typeFamily.triples(typeFamily.strings(), typeFamily.strings(), typeFamily.strings()));
    PCollection<Tuple3<String, String, String>> sorted = Sort.sortTriples(kv, first, second, third);
    List<Tuple3<String, String, String>> lines = Lists.newArrayList(sorted.materialize());
    Tuple3<String, String, String> l = lines.iterator().next();
    assertEquals(firstField, l.first());
    assertEquals(secondField, l.second());
    assertEquals(thirdField, l.third());
    pipeline.done();
  }

  private void runQuad(Pipeline pipeline, PTypeFamily typeFamily, ColumnOrder first, ColumnOrder second,
      ColumnOrder third, ColumnOrder fourth, String firstField, String secondField, String thirdField,
      String fourthField) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    PCollection<Tuple4<String, String, String, String>> kv = input.parallelDo(
        new DoFn<String, Tuple4<String, String, String, String>>() {
          @Override
          public void process(String input, Emitter<Tuple4<String, String, String, String>> emitter) {
            String[] split = input.split("[\t ]+");
            int len = split.length;
            emitter.emit(Tuple4.of(split[0], split[1 % len], split[2 % len], split[3 % len]));
          }
        }, typeFamily.quads(typeFamily.strings(), typeFamily.strings(), typeFamily.strings(), typeFamily.strings()));
    PCollection<Tuple4<String, String, String, String>> sorted = Sort.sortQuads(kv, first, second, third, fourth);
    Iterable<Tuple4<String, String, String, String>> lines = sorted.materialize();
    Tuple4<String, String, String, String> l = lines.iterator().next();
    assertEquals(firstField, l.first());
    assertEquals(secondField, l.second());
    assertEquals(thirdField, l.third());
    assertEquals(fourthField, l.fourth());
    pipeline.done();
  }

  private void runTupleN(Pipeline pipeline, PTypeFamily typeFamily, ColumnOrder[] orders, String[] fields)
      throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    PType[] types = new PType[orders.length];
    Arrays.fill(types, typeFamily.strings());
    PCollection<TupleN> kv = input.parallelDo(new DoFn<String, TupleN>() {
      @Override
      public void process(String input, Emitter<TupleN> emitter) {
        String[] split = input.split("[\t]+");
        emitter.emit(new TupleN(split));
      }
    }, typeFamily.tuples(types));
    PCollection<TupleN> sorted = Sort.sortTuples(kv, orders);
    Iterable<TupleN> lines = sorted.materialize();
    TupleN l = lines.iterator().next();
    int i = 0;
    for (String field : fields) {
      assertEquals(field, l.get(i++));
    }
    pipeline.done();
  }

  private void runTable(Pipeline pipeline, PTypeFamily typeFamily, String firstKey) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");

    PCollection<String> input = pipeline.readTextFile(inputPath);
    PTable<String, String> table = input.parallelDo(new DoFn<String, Pair<String, String>>() {
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        String[] split = input.split("[\t]+");
        emitter.emit(Pair.of(split[0], split[1]));
      }
    }, typeFamily.tableOf(typeFamily.strings(), typeFamily.strings()));

    PTable<String, String> sorted = Sort.sort(table);
    Iterable<Pair<String, String>> lines = sorted.materialize();
    Pair<String, String> l = lines.iterator().next();
    assertEquals(firstKey, l.first());
    pipeline.done();
  }

}
