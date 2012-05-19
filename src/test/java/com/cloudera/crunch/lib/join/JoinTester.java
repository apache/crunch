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
package com.cloudera.crunch.lib.join;

import java.io.IOException;
import java.io.Serializable;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.lib.Join;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
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

  protected PTable<String, Long> join(PCollection<String> w1, PCollection<String> w2,
        PTypeFamily ptf) {
    PTableType<String, Long> ntt = ptf.tableOf(ptf.strings(), ptf.longs());
    PTable<String, Long> ws1 = Aggregate.count(w1.parallelDo("ws1", new WordSplit(), ptf.strings()));
    PTable<String, Long> ws2 = Aggregate.count(w2.parallelDo("ws2", new WordSplit(), ptf.strings()));

    PTable<String, Pair<Long, Long>> join = Join.join(ws1, ws2, getJoinFn());

    PTable<String, Long> sums = join.parallelDo("cnt",
        new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>>() {
          @Override
          public void process(Pair<String, Pair<Long, Long>> input,
                              Emitter<Pair<String, Long>> emitter) {
            Pair<Long, Long> pair = input.second();
            long sum = (pair.first() != null ? pair.first() : 0) + (pair.second() != null ? pair.second() : 0);
            emitter.emit(Pair.of(input.first(), sum));
          }
        }, ntt);

    return sums;
  }

  protected void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String shakesInputPath = FileHelper.createTempCopyOf("shakes.txt");
    String maughamInputPath = FileHelper.createTempCopyOf("maugham.txt");

    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    PCollection<String> maugham = pipeline.readTextFile(maughamInputPath);
    PTable<String, Long> joined = join(shakespeare, maugham, typeFamily);
    Iterable<Pair<String, Long>> lines = joined.materialize();

    assertPassed(lines);

    pipeline.done();
  }

  @Test
   public void testWritableJoin() throws Exception {
    run(new MRPipeline(InnerJoinTest.class), WritableTypeFamily.getInstance());
  }
  
  @Test
  public void testAvroJoin() throws Exception {
    run(new MRPipeline(InnerJoinTest.class), AvroTypeFamily.getInstance());
  }

  /**
   * Used to check that the result of the join makes sense.
   *
   * @param lines The result of the join.
   */
  public abstract void assertPassed(Iterable<Pair<String, Long>> lines);

  /**
   * @return The JoinFn to use.
   */
  protected abstract JoinFn<String, Long, Long> getJoinFn();
}
