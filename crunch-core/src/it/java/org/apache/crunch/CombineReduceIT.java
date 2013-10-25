/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;

/**
 * Tests for two phase (combine and reduce) CombineFns.
 */
public class CombineReduceIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  private String docsPath;

  @Before
  public void setUp() throws Exception {
    docsPath = tmpDir.copyResourceFileName("docs.txt");
  }

  static class CountCombiner extends CombineFn<String,Long> {

    private final int stringLengthLimit;

    public CountCombiner(int stringLengthLimit) {
      this.stringLengthLimit = stringLengthLimit;
    }

    @Override
    public void process(Pair<String, Iterable<Long>> input, Emitter<Pair<String, Long>> emitter) {
      String key = input.first();
      if (key.length() <= stringLengthLimit) {
        long sum = 0L;
        for (Long countValue : input.second()) {
          sum += countValue;
        }
          emitter.emit(Pair.of(key, sum));
      } else {
        for (Long countValue : input.second()) {
          emitter.emit(Pair.of(key, countValue));
        }
      }
    }

  }

  @Test
  public void testCombineValues_NoCombineOrReduceOfLongWords() throws Exception {
    Iterable<Pair<String, Long>> mrResult = run(
        new MRPipeline(CombineReduceIT.class, tmpDir.getDefaultConfiguration()),
        docsPath, false, false);
    Iterable<Pair<String, Long>> memResult = run(
        MemPipeline.getInstance(), docsPath, false, false);
    Multiset<Pair<String, Long>> mrResultSet = ImmutableMultiset.copyOf(mrResult);
    Multiset<Pair<String, Long>> memResultSet = ImmutableMultiset.copyOf(memResult);

    assertEquals(mrResultSet, memResultSet);

    // Words with more than 3 characters shouldn't be combined at all
    assertTrue(mrResultSet.contains(Pair.of("this", 1L)));
    assertEquals(5, mrResultSet.count(Pair.of("this", 1L)));
  }

  @Test
  public void testCombineValues_OnlyReduceLongWords() throws Exception {
    Iterable<Pair<String, Long>> mrResult = run(
        new MRPipeline(CombineReduceIT.class, tmpDir.getDefaultConfiguration()),
        docsPath, false, true);
    Iterable<Pair<String, Long>> memResult = run(
        MemPipeline.getInstance(), docsPath, false, true);

    Multiset<Pair<String, Long>> mrResultSet = ImmutableMultiset.copyOf(mrResult);
    Multiset<Pair<String, Long>> memResultSet = ImmutableMultiset.copyOf(memResult);

    assertEquals(mrResultSet, memResultSet);

    // All words should be combined, although longer words will only
    // have been combined in the reduce phase
    assertTrue(mrResultSet.contains(Pair.of("this", 5L)));
    assertEquals(1, mrResultSet.count(Pair.of("this", 5L)));
  }

  @Test
  public void testCombineValues_CombineAndReduceLongWords() throws Exception {
    Iterable<Pair<String, Long>> mrResult = run(
        new MRPipeline(CombineReduceIT.class, tmpDir.getDefaultConfiguration()),
        docsPath, true, true);
    Iterable<Pair<String, Long>> memResult = run(
        MemPipeline.getInstance(), docsPath, true, true);

    Multiset<Pair<String, Long>> mrResultSet = ImmutableMultiset.copyOf(mrResult);
    Multiset<Pair<String, Long>> memResultSet = ImmutableMultiset.copyOf(memResult);

    assertEquals(mrResultSet, memResultSet);

    // All words should be combined, both in the combiner and reducer
    assertTrue(mrResultSet.contains(Pair.of("this", 5L)));
    assertEquals(1, mrResultSet.count(Pair.of("this", 5L)));
  }

  public static Iterable<Pair<String, Long>> run(Pipeline p, String inputPath, boolean combineLongWords, boolean reduceLongWords)
      throws Exception {
    return p.read(From.textFile(inputPath))
        .parallelDo("split", new DoFn<String, Pair<String, Long>>() {
          @Override
          public void process(String input, Emitter<Pair<String, Long>> emitter) {
            for (String word : input.split("\\s+")) {
              emitter.emit(Pair.of(word, 1L));
            }
          }
        }, Avros.tableOf(Avros.strings(), Avros.longs()))
        .groupByKey()
        .combineValues(
            new CountCombiner(combineLongWords ? Integer.MAX_VALUE : 3),
            new CountCombiner(reduceLongWords ? Integer.MAX_VALUE : 3))
        .materialize();
  }
}
