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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class SparkAggregatorIT {
  @Rule
  public TemporaryPath tempDir = new TemporaryPath();

  @Test
  public void testCount() throws Exception {
    SparkPipeline pipeline = new SparkPipeline("local", "aggregator");
    PCollection<String> set1 = pipeline.read(From.textFile(tempDir.copyResourceFileName("set1.txt")));
    PCollection<String> set2 = pipeline.read(From.textFile(tempDir.copyResourceFileName("set2.txt")));
    Iterable<Pair<Integer, Long>> cnts = set1.union(set2)
        .parallelDo(new CntFn(), Avros.ints())
        .count().materialize();
    assertEquals(ImmutableList.of(Pair.of(1, 7L)), Lists.newArrayList(cnts));
    pipeline.done();
  }

  @Test
  public void testAvroFirstN() throws Exception {
    SparkPipeline pipeline = new SparkPipeline("local", "aggregator");
    PCollection<String> set1 = pipeline.read(From.textFile(tempDir.copyResourceFileName("set1.txt"), Avros.strings()));
    PCollection<String> set2 = pipeline.read(From.textFile(tempDir.copyResourceFileName("set2.txt"), Avros.strings()));
    Aggregator<String> first5 = Aggregators.FIRST_N(5);
    Collection<String> aggregate = set1.union(set2).aggregate(first5).asCollection().getValue();
    pipeline.done();
    assertEquals(5, aggregate.size());
  }

  private static class CntFn extends MapFn<String, Integer> {
    @Override
    public Integer map(String input) {
      return 1;
    }
  }
}
