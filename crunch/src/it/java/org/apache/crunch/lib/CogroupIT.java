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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.fn.MapKeysFn;
import org.apache.crunch.fn.MapValuesFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.io.Files;

public class CogroupIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private static class WordSplit extends DoFn<String, Pair<String, Long>> {
    @Override
    public void process(String input, Emitter<Pair<String, Long>> emitter) {
      for (String word : Splitter.on(' ').split(input)) {
        emitter.emit(Pair.of(word, 1L));
      }
    }
  }

  public static PTable<String, Long> join(PCollection<String> w1, PCollection<String> w2, PTypeFamily ptf) {
    PTableType<String, Long> ntt = ptf.tableOf(ptf.strings(), ptf.longs());
    PTable<String, Long> ws1 = w1.parallelDo("ws1", new WordSplit(), ntt);
    PTable<String, Long> ws2 = w2.parallelDo("ws2", new WordSplit(), ntt);
    PTable<String, Pair<Collection<Long>, Collection<Long>>> cg = Cogroup.cogroup(ws1, ws2);
    PTable<String, Long> sums = cg.parallelDo("wc",
        new MapValuesFn<String, Pair<Collection<Long>, Collection<Long>>, Long>() {
          @Override
          public Long map(Pair<Collection<Long>, Collection<Long>> v) {
            long sum = 0L;
            for (Long value : v.first()) {
              sum += value;
            }
            for (Long value : v.second()) {
              sum += value;
            }
            return sum;
          }
        }, ntt);
    return sums.parallelDo("firstletters", new MapKeysFn<String, String, Long>() {
      @Override
      public String map(String k1) {
        if (k1.length() > 0) {
          return k1.substring(0, 1).toLowerCase();
        } else {
          return "";
        }
      }
    }, ntt).groupByKey().combineValues(Aggregators.SUM_LONGS());
  }

  @Test
  public void testWritableJoin() throws Exception {
    run(new MRPipeline(CogroupIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvroJoin() throws Exception {
    run(new MRPipeline(CogroupIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance());
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    String maughamInputPath = tmpDir.copyResourceFileName("maugham.txt");
    File output = tmpDir.getFile("output");

    PCollection<String> shakespeare = pipeline.read(From.textFile(shakesInputPath));
    PCollection<String> maugham = pipeline.read(From.textFile(maughamInputPath));
    pipeline.writeTextFile(join(shakespeare, maugham, typeFamily), output.getAbsolutePath());
    pipeline.done();

    File outputFile = new File(output, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if (line.equals("j\t705")) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }
}
