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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.junit.Test;

import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;

@SuppressWarnings("serial")
public class TermFrequencyTest implements Serializable {  
  
  @Test
  public void testTermFrequencyWithNoTransform() throws IOException {
    run(new MRPipeline(TermFrequencyTest.class), WritableTypeFamily.getInstance(), false);
  }
  
  @Test
  public void testTermFrequencyWithTransform() throws IOException {
    run(new MRPipeline(TermFrequencyTest.class), WritableTypeFamily.getInstance(), true);
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
    String input = FileHelper.createTempCopyOf("docs.txt");
    
    File transformedOutput = FileHelper.createOutputPath();
    File tfOutput = FileHelper.createOutputPath();
    
    PCollection<String> docs = pipeline.readTextFile(input);
    
    PTypeFamily ptf = docs.getTypeFamily();
    
    /*
     * Input: String
     * Input title  text
     * 
     * Output: PTable<Pair<String, String>, Long> 
     * Pair<Pair<word, title>, count in title>
     */
    PTable<Pair<String, String>, Long> tf = Aggregate.count(docs.parallelDo("term document frequency",
        new DoFn<String, Pair<String, String>>() {
      @Override
      public void process(String doc, Emitter<Pair<String, String>> emitter) {
        String[] kv = doc.split("\t");
        String title = kv[0];
        String text = kv[1];
        for (String word : text.split("\\W+")) {
          if(word.length() > 0) {
            Pair<String, String> pair = Pair.of(word.toLowerCase(), title);
            emitter.emit(pair);
          }
        }
      }
    }, ptf.pairs(ptf.strings(), ptf.strings())));
    
    if(transformTF) {
      /*
       * Input: Pair<Pair<String, String>, Long>
       * Pair<Pair<word, title>, count in title>
       * 
       * Output: PTable<String, Pair<String, Long>>
       * PTable<word, Pair<title, count in title>>
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
    Iterable<String> lines = ((ReadableSourceTarget<String>) st).read(pipeline.getConfiguration());
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
