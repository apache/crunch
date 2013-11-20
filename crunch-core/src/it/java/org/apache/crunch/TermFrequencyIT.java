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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class TermFrequencyIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testTermFrequencyWithNoTransform() throws IOException {
    run(new MRPipeline(TermFrequencyIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), false);
  }

  @Test
  public void testTermFrequencyWithTransform() throws IOException {
    run(new MRPipeline(TermFrequencyIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance(), true);
  }

  @Test
  public void testTermFrequencyNoTransformInMemory() throws IOException {
    run(MemPipeline.getInstance(), WritableTypeFamily.getInstance(), false);
  }

  @Test
  public void testTermFrequencyWithTransformInMemory() throws IOException {
    run(MemPipeline.getInstance(), WritableTypeFamily.getInstance(), true);
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily, boolean transformTF) throws IOException {
    String input = tmpDir.copyResourceFileName("docs.txt");

    File transformedOutput = tmpDir.getFile("transformed-output");
    File tfOutput = tmpDir.getFile("tf-output");

    PCollection<String> docs = pipeline.readTextFile(input);

    PTypeFamily ptf = docs.getTypeFamily();

    /*
     * Input: String Input title text
     *
     * Output: PTable<Pair<String, String>, Long> Pair<Pair<word, title>, count
     * in title>
     */
    PTable<Pair<String, String>, Long> tf = Aggregate.count(docs.parallelDo("term document frequency",
        new DoFn<String, Pair<String, String>>() {
          @Override
          public void process(String doc, Emitter<Pair<String, String>> emitter) {
            String[] kv = doc.split("\t");
            String title = kv[0];
            String text = kv[1];
            for (String word : text.split("\\W+")) {
              if (!word.isEmpty()) {
                Pair<String, String> pair = Pair.of(word.toLowerCase(Locale.ENGLISH), title);
                emitter.emit(pair);
              }
            }
          }
        }, ptf.pairs(ptf.strings(), ptf.strings())));

    if (transformTF) {
      /*
       * Input: Pair<Pair<String, String>, Long> Pair<Pair<word, title>, count
       * in title>
       *
       * Output: PTable<String, Pair<String, Long>> PTable<word, Pair<title,
       * count in title>>
       */
      PTable<String, Pair<String, Long>> wordDocumentCountPair = tf.parallelDo("transform wordDocumentPairCount",
          new MapFn<Pair<Pair<String, String>, Long>, Pair<String, Pair<String, Long>>>() {
            @Override
            public Pair<String, Pair<String, Long>> map(Pair<Pair<String, String>, Long> input) {
              Pair<String, String> wordDocumentPair = input.first();
              return Pair.of(wordDocumentPair.first(), Pair.of(wordDocumentPair.second(), input.second()));
            }
          }, ptf.tableOf(ptf.strings(), ptf.pairs(ptf.strings(), ptf.longs())));

      pipeline.writeTextFile(wordDocumentCountPair, transformedOutput.getAbsolutePath());
    }

    SourceTarget<String> st = At.textFile(tfOutput.getAbsolutePath());
    pipeline.write(tf, st);

    pipeline.run();

    // test the case we should see
    Iterable<String> lines = ((ReadableSource<String>) st).read(pipeline.getConfiguration());
    boolean passed = false;
    for (String line : lines) {
      if ("[well,A]\t0".equals(line)) {
        fail("Found " + line + " but well is in Document A 1 time");
      }
      if ("[well,A]\t1".equals(line)) {
        passed = true;
      }
    }
    assertTrue(passed);
    pipeline.done();
  }
}
