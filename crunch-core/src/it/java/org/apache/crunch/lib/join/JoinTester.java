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
package org.apache.crunch.lib.join;

import java.io.IOException;
import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

public abstract class JoinTester implements Serializable {
  
  private static class WordSplit extends DoFn<String, String> {
    @Override
    public void process(String input, Emitter<String> emitter) {
      for (String word : input.split("\\s+")) {
        emitter.emit(word);
      }
    }
  }

  protected PTable<String, Long> join(PCollection<String> w1, PCollection<String> w2, PTypeFamily ptf) {
    PTableType<String, Long> ntt = ptf.tableOf(ptf.strings(), ptf.longs());
    PTable<String, Long> ws1 = Aggregate.count(w1.parallelDo("ws1", new WordSplit(), ptf.strings()));
    PTable<String, Long> ws2 = Aggregate.count(w2.parallelDo("ws2", new WordSplit(), ptf.strings()));
    
    JoinStrategy<String,Long,Long> joinStrategy = getJoinStrategy();

    PTable<String, Pair<Long, Long>> join = joinStrategy.join(ws1, ws2, getJoinType());

    PTable<String, Long> sums = join.parallelDo("cnt", new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>>() {
      @Override
      public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Long>> emitter) {
        Pair<Long, Long> pair = input.second();
        long sum = (pair.first() != null ? pair.first() : 0) + (pair.second() != null ? pair.second() : 0);
        emitter.emit(Pair.of(input.first(), sum));
      }
    }, ntt);

    return sums;
  }

  protected void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    String maughamInputPath = tmpDir.copyResourceFileName("maugham.txt");

    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    PCollection<String> maugham = pipeline.readTextFile(maughamInputPath);
    PTable<String, Long> joined = join(shakespeare, maugham, typeFamily);
    Iterable<Pair<String, Long>> lines = joined.materialize();

    assertPassed(lines);

    pipeline.done();
  }
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testWritableJoin() throws Exception {
    run(new MRPipeline(AbstractInnerJoinIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvroJoin() throws Exception {
    run(new MRPipeline(AbstractInnerJoinIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance());
  }
  
  @Test
  public void testAvroJoin_MemPipeline() throws Exception {
    run(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
  }
  
  @Test
  public void testWritableJoin_MemPipeline() throws Exception {
    run(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
  }
  
  /** 
   * Return the JoinStrategy to be tested. 
   */
  protected abstract <K, U, V> JoinStrategy<K, U, V> getJoinStrategy();
  

  /**
   * Used to check that the result of the join makes sense.
   * 
   * @param lines
   *          The result of the join.
   */
  public abstract void assertPassed(Iterable<Pair<String, Long>> lines);

  /**
   * @return The JoinType to be used in the test.
   */
  protected abstract JoinType getJoinType();
}
