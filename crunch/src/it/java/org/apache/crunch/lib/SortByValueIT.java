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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 *
 */
public class SortByValueIT {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();
  
  private static class SplitFn extends MapFn<String, Pair<String, Long>> {
    private String sep;
    
    public SplitFn(String sep) {
      this.sep = sep;
    }
    
    @Override
    public Pair<String, Long> map(String input) {
      String[] pieces = input.split(sep);
      return Pair.of(pieces[0], Long.valueOf(pieces[1]));
    }
  }
  
  @Test
  public void testSortByValueWritables() throws Exception {
    run(new MRPipeline(SortByValueIT.class), WritableTypeFamily.getInstance());
  }
  
  @Test
  public void testSortByValueAvro() throws Exception {
    run(new MRPipeline(SortByValueIT.class), AvroTypeFamily.getInstance());
  }
  
  public void run(Pipeline pipeline, PTypeFamily ptf) throws Exception {
    String sbv = tmpDir.copyResourceFileName("sort_by_value.txt");
    PTable<String, Long> letterCounts = pipeline.read(From.textFile(sbv)).parallelDo(new SplitFn("\t"),
        ptf.tableOf(ptf.strings(), ptf.longs()));
    PCollection<Pair<String, Long>> sorted = Sort.sortPairs(
        letterCounts,
        new ColumnOrder(2, Order.DESCENDING),
        new ColumnOrder(1, Order.ASCENDING));
    assertEquals(
        ImmutableList.of(Pair.of("C", 3L), Pair.of("A", 2L), Pair.of("D", 2L), Pair.of("B", 1L), Pair.of("E", 1L)),
        ImmutableList.copyOf(sorted.materialize()));
  }
}
