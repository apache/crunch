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
package com.cloudera.crunch;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.writable.Writables;

public class TextPairTest  {

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(TextPairTest.class));
  }
  
  private static final String CANARY = "Writables.STRING_TO_TEXT";
  
  public static PCollection<Pair<String, String>> wordDuplicate(PCollection<String> words) {
    return words.parallelDo("my word duplicator", new DoFn<String, Pair<String, String>>() {
      private static final long serialVersionUID = 1L;
      @Override
      public void process(String line, Emitter<Pair<String, String>> emitter) {
        for (String word : line.split("\\W+")) {
          if(word.length() > 0) {
            Pair<String, String> pair = Pair.of(CANARY, word);
            emitter.emit(pair);
          }
        }
      }
    }, Writables.pairs(Writables.strings(), Writables.strings()));
  }
  
  public void run(Pipeline pipeline) throws IOException {
    String input = FileHelper.createTempCopyOf("shakes.txt");
        
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
