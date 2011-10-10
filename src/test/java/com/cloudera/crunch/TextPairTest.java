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

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.cloudera.crunch.type.writable.Writables;
import com.google.common.io.Files;

public class TextPairTest  {

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(TextPairTest.class), WritableTypeFamily.getInstance());
  }
  
  private static final String CANARY = "Writables.STRING_TO_TEXT";
  
  public static PCollection<Pair<String, String>> wordDuplicate(PCollection<String> words, PTypeFamily typeFamily) {
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
  
  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    File input = File.createTempFile("shakes", "txt");
    input.deleteOnExit();
    Files.copy(newInputStreamSupplier(getResource("shakes.txt")), input);
    
    File output = File.createTempFile("output", "");
    String outputPath = output.getAbsolutePath();
    output.delete();
    
    PCollection<String> shakespeare = pipeline.readTextFile(input.getAbsolutePath());
    pipeline.writeTextFile(wordDuplicate(shakespeare, typeFamily), outputPath);
    pipeline.done();
    
    File outputFile = new File(output, "part-m-00000");
    outputFile.deleteOnExit();
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if (line.contains(CANARY)) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }  
}
