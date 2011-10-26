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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.avro.AvroTypeFamily;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.google.common.collect.Iterables;

public class AggregateTest {

  @Test public void testWritables() throws Exception {
    runMinMax(new MRPipeline(AggregateTest.class), WritableTypeFamily.getInstance());
  }

  @Test public void testAvro() throws Exception {
    runMinMax(new MRPipeline(AggregateTest.class), AvroTypeFamily.getInstance());
  }

  public static void runMinMax(Pipeline pipeline, PTypeFamily family) throws Exception {
    String shakesInputPath = FileHelper.createTempCopyOf("shakes.txt");
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
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
    pipeline.done();
  }
}
