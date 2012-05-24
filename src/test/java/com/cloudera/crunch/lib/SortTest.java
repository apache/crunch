/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.lib;

import static com.cloudera.crunch.lib.Sort.ColumnOrder.by;
import static com.cloudera.crunch.lib.Sort.Order.*;
import static org.junit.Assert.assertEquals;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Sort.ColumnOrder;
import com.cloudera.crunch.lib.Sort.Order;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

public class SortTest implements Serializable {
  
  @Test
  public void testWritableSortAsc() throws Exception {
    runSingle(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        Order.ASCENDING, "A\tand this text as well");
  }

  @Test
  public void testWritableSortDesc() throws Exception {
    runSingle(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        Order.DESCENDING, "B\tthis doc has some text");
  }
  
  @Test
  public void testWritableSortAscDesc() throws Exception {
    runPair(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), "A", "this doc has this text");
  }

  @Test
  public void testWritableSortSecondDescFirstDesc() throws Exception {
    runPair(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        by(2, DESCENDING), by(1, ASCENDING), "A", "this doc has this text");
  }

  @Test
  public void testWritableSortTripleAscDescAsc() throws Exception {
    runTriple(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), by(3, ASCENDING), "A", "this", "doc");
  }

  @Test
  public void testWritableSortQuadAscDescAscDesc() throws Exception {
    runQuad(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), by(3, ASCENDING), by(4, DESCENDING), "A", "this", "doc", "has");
  }

  @Test
  public void testWritableSortTupleNAscDesc() throws Exception {
    runTupleN(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        new ColumnOrder[] { by(1, ASCENDING), by(2, DESCENDING)}, new String[] { "A", "this doc has this text" });
  }

  @Test
  public void testWritableSortTable() throws Exception {
    runTable(new MRPipeline(SortTest.class), WritableTypeFamily.getInstance(),
        "A");
  }
  
  @Test
  public void testAvroSortAsc() throws Exception {
    runSingle(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        Order.ASCENDING, "A\tand this text as well");
  }
  
  @Test
  public void testAvroSortDesc() throws Exception {
    runSingle(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        Order.DESCENDING, "B\tthis doc has some text");
  }
  
  @Test
  public void testAvroSortPairAscAsc() throws Exception {
    runPair(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), "A", "this doc has this text");
  }
  
  @Test
  @Ignore("Avro sorting only works in field order at the moment")
  public void testAvroSortPairSecondAscFirstDesc() throws Exception {
    runPair(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        by(2, DESCENDING), by(1, ASCENDING), "A", "this doc has this text");
  }
  
  @Test
  public void testAvroSortTripleAscDescAsc() throws Exception {
    runTriple(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), by(3, ASCENDING), "A", "this", "doc");
  }

  @Test
  public void testAvroSortQuadAscDescAscDesc() throws Exception {
    runQuad(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        by(1, ASCENDING), by(2, DESCENDING), by(3, ASCENDING), by(4, DESCENDING), "A", "this", "doc", "has");
  }

  @Test
  public void testAvroSortTupleNAscDesc() throws Exception {
    runTupleN(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(),
        new ColumnOrder[] { by(1, ASCENDING), by(2, DESCENDING) }, new String[] { "A", "this doc has this text" });
  }
  
  @Test
  public void testAvroSortTable() throws Exception {
    runTable(new MRPipeline(SortTest.class), AvroTypeFamily.getInstance(), "A");
  }

  private void runSingle(Pipeline pipeline, PTypeFamily typeFamily,
      Order order, String firstLine) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
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
  
  private void runPair(Pipeline pipeline, PTypeFamily typeFamily,
      ColumnOrder first, ColumnOrder second, String firstField, String secondField) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
    PCollection<String> input = pipeline.readTextFile(inputPath);
    PCollection<Pair<String, String>> kv = input.parallelDo(
      new DoFn<String, Pair<String, String>>() {
        @Override
        public void process(String input, Emitter<Pair<String, String>> emitter) {
          String[] split = input.split("[\t]+");
          emitter.emit(Pair.of(split[0], split[1]));
        }
    }, typeFamily.pairs(typeFamily.strings(), typeFamily.strings()));
    PCollection<Pair<String, String>> sorted = Sort.sortPairs(kv, first, second);
    Iterable<Pair<String, String>> lines = sorted.materialize();
    Pair<String, String> l = lines.iterator().next();
    assertEquals(firstField, l.first());
    assertEquals(secondField, l.second());
    pipeline.done();
  }
  
  private void runTriple(Pipeline pipeline, PTypeFamily typeFamily,
      ColumnOrder first, ColumnOrder second, ColumnOrder third, String firstField, String secondField, String thirdField) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
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
    Iterable<Tuple3<String, String, String>> lines = sorted.materialize();
    Tuple3<String, String, String> l = lines.iterator().next();
    assertEquals(firstField, l.first());
    assertEquals(secondField, l.second());
    assertEquals(thirdField, l.third());
    pipeline.done();
  }
  
  private void runQuad(Pipeline pipeline, PTypeFamily typeFamily,
      ColumnOrder first, ColumnOrder second, ColumnOrder third, ColumnOrder fourth,
      String firstField, String secondField, String thirdField, String fourthField) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
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
  
  private void runTupleN(Pipeline pipeline, PTypeFamily typeFamily,
      ColumnOrder[] orders, String[] fields) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
    PCollection<String> input = pipeline.readTextFile(inputPath);
    PType[] types = new PType[orders.length];
    Arrays.fill(types, typeFamily.strings());
    PCollection<TupleN> kv = input.parallelDo(
      new DoFn<String, TupleN>() {
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

  private void runTable(Pipeline pipeline, PTypeFamily typeFamily,
      String firstKey) throws IOException {
    String inputPath = FileHelper.createTempCopyOf("docs.txt");
    
    PCollection<String> input = pipeline.readTextFile(inputPath);
    PTable<String, String> table = input.parallelDo(
        new DoFn<String, Pair<String, String>>() {
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
