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
import static org.junit.Assert.assertEquals;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 *
 */
public class DeepCopyCustomTuplesIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  public static class PID extends Pair<Integer, String> {
    public PID(Integer first, String second) {
      super(first, second);
    }
  }
  
  private static PType<PID> pids = tuples(PID.class, ints(), strings());
  
  @Test
  public void testDeepCopyCustomTuple() throws Exception {
    Pipeline p = new MRPipeline(DeepCopyCustomTuplesIT.class, tmpDir.getDefaultConfiguration());
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakes = p.readTextFile(shakesInputPath);
    Iterable<String> out = shakes
        .parallelDo(new PreProcFn(), tableOf(ints(), pairs(ints(), pids)))
        .groupByKey()
        .parallelDo(new PostProcFn(), strings())
        .materialize();
    assertEquals(65, Iterables.size(out));
    p.done();
  }
  
  private static class PreProcFn extends MapFn<String, Pair<Integer, Pair<Integer, PID>>> {
    private int counter = 0;
    @Override
    public Pair<Integer, Pair<Integer, PID>> map(String input) {
      return Pair.of(counter++, Pair.of(counter++, new PID(input.length(), input)));
    }
  };
  
  private static class PostProcFn extends DoFn<Pair<Integer, Iterable<Pair<Integer, PID>>>, String> {
    @Override
    public void process(Pair<Integer, Iterable<Pair<Integer, PID>>> input, Emitter<String> emitter) {
      for (Pair<Integer, PID> p : input.second()) {
        if (p.second().first() > 0 && p.second().first() < 10) {
          emitter.emit(p.second().second());
        }
      }
    }
  }
}
