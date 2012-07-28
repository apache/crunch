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

import java.io.IOException;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

public class TextPairIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(TextPairIT.class, tmpDir.getDefaultConfiguration()));
  }

  private static final String CANARY = "Writables.STRING_TO_TEXT";

  public static PCollection<Pair<String, String>> wordDuplicate(PCollection<String> words) {
    return words.parallelDo("my word duplicator", new DoFn<String, Pair<String, String>>() {
      public void process(String line, Emitter<Pair<String, String>> emitter) {
        for (String word : line.split("\\W+")) {
          if (word.length() > 0) {
            Pair<String, String> pair = Pair.of(CANARY, word);
            emitter.emit(pair);
          }
        }
      }
    }, Writables.pairs(Writables.strings(), Writables.strings()));
  }

  public void run(Pipeline pipeline) throws IOException {
    String input = tmpDir.copyResourceFileName("shakes.txt");

    PCollection<String> shakespeare = pipeline.read(From.textFile(input));
    Iterable<Pair<String, String>> lines = pipeline.materialize(wordDuplicate(shakespeare));
    boolean passed = false;
    for (Pair<String, String> line : lines) {
      if (line.first().contains(CANARY)) {
        passed = true;
        break;
      }
    }

    pipeline.done();
    assertTrue(passed);
  }
}
