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

import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.Employee;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class AggregateIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testWritables() throws Exception {
    Pipeline pipeline = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
    runMinMax(shakes, WritableTypeFamily.getInstance());
    pipeline.done();
  }

  @Test
  public void testAvro() throws Exception {
    Pipeline pipeline = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
    runMinMax(shakes, AvroTypeFamily.getInstance());
    pipeline.done();
  }

  @Test
  public void testInMemoryAvro() throws Exception {
    PCollection<String> someText = MemPipeline.collectionOf("first line", "second line", "third line");
    runMinMax(someText, AvroTypeFamily.getInstance());
  }

  public static void runMinMax(PCollection<String> shakes, PTypeFamily family) throws Exception {
    PCollection<Integer> lengths = shakes.parallelDo(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, family.ints());
    PCollection<Integer> negLengths = lengths.parallelDo(new MapFn<Integer, Integer>() {
      @Override
      public Integer map(Integer input) {
        return -input;
      }
    }, family.ints());
    Integer maxLengths = Aggregate.max(lengths).getValue();
    Integer minLengths = Aggregate.min(negLengths).getValue();
    assertTrue(maxLengths != null);
    assertTrue(minLengths != null);
    assertEquals(maxLengths.intValue(), -minLengths.intValue());
  }

  private static class SplitFn extends MapFn<String, Pair<String, String>> {
    @Override
    public Pair<String, String> map(String input) {
      String[] p = input.split("\\s+");
      return Pair.of(p[0], p[1]);
    }
  }

  @Test
  public void testCollectUrls() throws Exception {
    Pipeline p = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    String urlsInputPath = tmpDir.copyResourceFileName("urls.txt");
    PTable<String, Collection<String>> urls = Aggregate.collectValues(p.readTextFile(urlsInputPath).parallelDo(
        new SplitFn(), tableOf(strings(), strings())));
    for (Pair<String, Collection<String>> e : urls.materialize()) {
      String key = e.first();
      int expectedSize = 0;
      if ("www.A.com".equals(key)) {
        expectedSize = 4;
      } else if ("www.B.com".equals(key) || "www.F.com".equals(key)) {
        expectedSize = 2;
      } else if ("www.C.com".equals(key) || "www.D.com".equals(key) || "www.E.com".equals(key)) {
        expectedSize = 1;
      }
      assertEquals("Checking key = " + key, expectedSize, e.second().size());
      p.done();
    }
  }

  @Test
  public void testTopN() throws Exception {
    PTableType<String, Integer> ptype = Avros.tableOf(Avros.strings(), Avros.ints());
    PTable<String, Integer> counts = MemPipeline.typedTableOf(ptype, "foo", 12, "bar", 17, "baz", 29);

    PTable<String, Integer> top2 = Aggregate.top(counts, 2, true);
    assertEquals(ImmutableList.of(Pair.of("baz", 29), Pair.of("bar", 17)), top2.materialize());

    PTable<String, Integer> bottom2 = Aggregate.top(counts, 2, false);
    assertEquals(ImmutableList.of(Pair.of("foo", 12), Pair.of("bar", 17)), bottom2.materialize());
  }

  @Test
  public void testTopN_MRPipeline() throws IOException {
    Pipeline pipeline = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    PTable<StringWrapper, String> entries = pipeline
        .read(From.textFile(tmpDir.copyResourceFileName("set1.txt"), Avros.strings()))
        .by(new StringWrapper.StringToStringWrapperMapFn(), Avros.reflects(StringWrapper.class));
    PTable<StringWrapper, String> topEntries = Aggregate.top(entries, 3, true);
    List<Pair<StringWrapper, String>> expectedTop3 = Lists.newArrayList(
        Pair.of(StringWrapper.wrap("e"), "e"),
        Pair.of(StringWrapper.wrap("c"), "c"),
        Pair.of(StringWrapper.wrap("b"), "b"));

    assertEquals(
        expectedTop3,
        Lists.newArrayList(topEntries.materialize()));

  }

  @Test
  public void testCollectValues_Writables() throws IOException {
    Pipeline pipeline = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    Map<Integer, Collection<Text>> collectionMap = pipeline.readTextFile(tmpDir.copyResourceFileName("set2.txt"))
        .parallelDo(new MapStringToTextPair(), Writables.tableOf(Writables.ints(), Writables.writables(Text.class)))
        .collectValues().materializeToMap();

    assertEquals(1, collectionMap.size());

    assertTrue(collectionMap.get(1).containsAll(Lists.newArrayList(new Text("c"), new Text("d"), new Text("a"))));
  }

  @Test
  public void testCollectValues_Avro() throws IOException {

    MapStringToEmployeePair mapFn = new MapStringToEmployeePair();
    Pipeline pipeline = new MRPipeline(AggregateIT.class, tmpDir.getDefaultConfiguration());
    Map<Integer, Collection<Employee>> collectionMap = pipeline.readTextFile(tmpDir.copyResourceFileName("set2.txt"))
        .parallelDo(mapFn, Avros.tableOf(Avros.ints(), Avros.records(Employee.class))).collectValues()
        .materializeToMap();

    assertEquals(1, collectionMap.size());

    Employee empC = mapFn.map("c").second();
    Employee empD = mapFn.map("d").second();
    Employee empA = mapFn.map("a").second();

    assertTrue(collectionMap.get(1).containsAll(Lists.newArrayList(empC, empD, empA)));
  }

  private static class MapStringToTextPair extends MapFn<String, Pair<Integer, Text>> {
    @Override
    public Pair<Integer, Text> map(String input) {
      return Pair.of(1, new Text(input));
    }
  }

  private static class MapStringToEmployeePair extends MapFn<String, Pair<Integer, Employee>> {
    @Override
    public Pair<Integer, Employee> map(String input) {
      Employee emp = new Employee();
      emp.name = input;
      emp.salary = 0;
      emp.department = "";
      return Pair.of(1, emp);
    }
  }

  public static class PojoText {
    private String value;

    public PojoText() {
      this("");
    }

    public PojoText(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("PojoText<%s>", this.value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PojoText other = (PojoText) obj;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }

  }
}
