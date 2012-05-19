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

import static com.cloudera.crunch.types.writable.Writables.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.avro.Avros;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class AggregateTest {

  @Test public void testWritables() throws Exception {
    Pipeline pipeline = new MRPipeline(AggregateTest.class);
    String shakesInputPath = FileHelper.createTempCopyOf("shakes.txt");
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
    runMinMax(shakes, WritableTypeFamily.getInstance());
    pipeline.done();
  }

  @Test public void testAvro() throws Exception {
    Pipeline pipeline = new MRPipeline(AggregateTest.class);
    String shakesInputPath = FileHelper.createTempCopyOf("shakes.txt");
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
    runMinMax(shakes, AvroTypeFamily.getInstance());
    pipeline.done();
  }

  @Test public void testInMemoryAvro() throws Exception {
    PCollection<String> someText = MemPipeline.collectionOf(
        "first line", "second line", "third line");
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
    Integer maxLengths = Iterables.getFirst(Aggregate.max(lengths).materialize(), null);
    Integer minLengths = Iterables.getFirst(Aggregate.min(negLengths).materialize(), null);
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
  
  @Test public void testCollectUrls() throws Exception {
    Pipeline p = new MRPipeline(AggregateTest.class);
    String urlsInputPath = FileHelper.createTempCopyOf("urls.txt");
    PTable<String, Collection<String>> urls = Aggregate.collectValues(
        p.readTextFile(urlsInputPath)
        .parallelDo(new SplitFn(), tableOf(strings(), strings())));
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
  
  @Test public void testTopN() throws Exception {
    PTableType<String, Integer> ptype = Avros.tableOf(Avros.strings(), Avros.ints());
    PTable<String, Integer> counts = MemPipeline.typedTableOf(ptype, "foo", 12, "bar", 17, "baz", 29);
    
    PTable<String, Integer> top2 = Aggregate.top(counts, 2, true);
    assertEquals(ImmutableList.of(Pair.of("baz", 29), Pair.of("bar", 17)), top2.materialize());
    
    PTable<String, Integer> bottom2 = Aggregate.top(counts, 2, false);
    assertEquals(ImmutableList.of(Pair.of("foo", 12), Pair.of("bar", 17)), bottom2.materialize());
  }
}
