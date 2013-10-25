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

import static org.apache.crunch.types.avro.Avros.*;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

import java.util.Locale;

/**
 * Verifies that complex plans execute dependent jobs in the correct sequence.
 */
public class LongPipelinePlannerIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test
  public void testMR() throws Exception {
    run(new MRPipeline(LongPipelinePlannerIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("shakes.txt"),
        tmpDir.getFileName("output"));
  }
  
  public static void run(Pipeline p, String input, String output) {
    PCollection<String> in = p.readTextFile(input);
    PCollection<String> toLower = in.parallelDo("tolower", new DoFn<String, String>() {
      @Override
      public void process(String input, Emitter<String> emitter) {
        emitter.emit(input.toLowerCase(Locale.ENGLISH));
      }
    }, strings());

    PTable<Integer, String> keyedLower = toLower.parallelDo("keyed", new MapFn<String, Pair<Integer, String>>() {
      @Override
      public Pair<Integer, String> map(String input) {
        return Pair.of(input.length(), input);
      }
    }, tableOf(ints(), strings())).groupByKey().ungroup();

    PCollection<String> iso = keyedLower.groupByKey().parallelDo("iso", new DoFn<Pair<Integer, Iterable<String>>, String>() {
      @Override
      public void process(Pair<Integer, Iterable<String>> input, Emitter<String> emitter) {
        for (String s : input.second()) {
          emitter.emit(s);
        }
      } 
    }, strings());

    ReadableData<String> isoRD = iso.asReadable(true);
    ParallelDoOptions.Builder builder = ParallelDoOptions.builder().sourceTargets(isoRD.getSourceTargets());

    PTable<Integer, String> splitMap = keyedLower.parallelDo("split-map",
        new MapFn<Pair<Integer, String>, Pair<Integer, String>>() {
      @Override
      public Pair<Integer, String> map(Pair<Integer, String> input) {
        return input;
      }
    }, tableOf(ints(), strings()), builder.build());

    PTable<Integer, String> splitReduce = splitMap.groupByKey().parallelDo("split-reduce",
        new DoFn<Pair<Integer, Iterable<String>>, Pair<Integer, String>>() {
      @Override
      public void process(Pair<Integer, Iterable<String>> input,
          Emitter<Pair<Integer, String>> emitter) {
        emitter.emit(Pair.of(input.first(), input.second().iterator().next()));
      }
    }, tableOf(ints(), strings()));

    PTable<Integer, String> splitReduceResetKeys = splitReduce.parallelDo("reset",
        new MapFn<Pair<Integer, String>, Pair<Integer, String>>() {
      @Override
      public Pair<Integer, String> map(Pair<Integer, String> input) {
        return Pair.of(input.first() - 1, input.second());
      }
    }, tableOf(ints(), strings()));
    PTable<Integer, String> intersections = splitReduceResetKeys.groupByKey().ungroup();

    PCollection<String> merged = intersections.values();
    PCollection<String> upper = merged.parallelDo("toupper", new MapFn<String, String>() {
      @Override
      public String map(String input) {
        return input.toUpperCase(Locale.ENGLISH);
      }
    }, strings());

    p.writeTextFile(upper, output);
    p.done();
  }
}
