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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SparkTaskAttemptIT {
  @Rule
  public TemporaryPath tempDir = new TemporaryPath();

  private SparkPipeline pipeline;

  @Before
  public void setUp() throws IOException {
    pipeline = new SparkPipeline("local", "taskattempt");
  }

  @After
  public void tearDown() throws Exception {
    pipeline.done();
  }

  @Test
  public void testTaskAttempts() throws Exception {
    String inputPath = tempDir.copyResourceFileName("set1.txt");
    String inputPath2 = tempDir.copyResourceFileName("set2.txt");

    PCollection<String> first = pipeline.read(From.textFile(inputPath));
    PCollection<String> second = pipeline.read(From.textFile(inputPath2));

    Iterable<Pair<Integer, Long>> cnts = first.union(second)
        .parallelDo(new TaskMapFn(), Avros.ints())
        .count()
        .materialize();
    assertEquals(ImmutableSet.of(Pair.of(0, 4L), Pair.of(1, 3L)), Sets.newHashSet(cnts));
  }

  private static class TaskMapFn extends MapFn<String, Integer> {
    @Override
    public Integer map(String input) {
      return getContext().getTaskAttemptID().getTaskID().getId();
    }
  }
}
