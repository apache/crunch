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
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.cloudera.crunch.fn.MapKeysFn;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.seq.SeqFileSourceTarget;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.lib.Join;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.cloudera.crunch.type.writable.Writables;
import com.google.common.io.Files;

@SuppressWarnings("serial")
public class TFIDFTest implements Serializable {  
  
  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(TFIDFTest.class), WritableTypeFamily.getInstance());
  }
  
  /**
   * This method should generate a TF-IDF score for the input.
   */
  public PTable<String, Pair<Long, Pair<String, Long>>> generateTFIDF(PCollection<String> docs,
      Path termFreqPath, PTypeFamily ptf) throws IOException {    
    PTable<Pair<String, String>, Long> tf = Aggregate.count(docs.parallelDo("term frequency",
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
    tf.writeTo(new SeqFileSourceTarget(termFreqPath, tf.getPType()));
    
    PTable<String, Long> n = Aggregate.count(tf.parallelDo("little n (# of docs a word is in)",  
        new DoFn<Pair<Pair<String, String>, Long>, String>() {
      @Override
      public void process(Pair<Pair<String, String>, Long> input,
          Emitter<String> emitter) {
        emitter.emit(input.first().first());
      }
    }, Writables.strings()));
    
    PTable<String, Pair<String, Long>> wordDocumentCountPair = tf.parallelDo("transform wordDocumentPairCount",
        new MapFn<Pair<Pair<String, String>, Long>, Pair<String, Pair<String, Long>>>() {
          @Override
          public Pair<String, Pair<String, Long>> map(Pair<Pair<String, String>, Long> input) {
            Pair<String, String> wordDocumentPair = input.first();            
            return Pair.of(wordDocumentPair.first(), Pair.of(wordDocumentPair.second(), input.second()));
          }
      }, ptf.tableOf(ptf.strings(), ptf.pairs(ptf.strings(), ptf.longs())));
        
    PTable<String, Pair<Long, Pair<String, Long>>>  joinedResults = Join.join(n, wordDocumentCountPair);
    return joinedResults;
  }
  
  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    File input = File.createTempFile("docs", "txt");
    input.deleteOnExit();
    Files.copy(newInputStreamSupplier(getResource("docs.txt")), input);
    
    File output = File.createTempFile("output", "");
    String outputPath1 = output.getAbsolutePath();
    output.delete();
    output = File.createTempFile("output", "");
    String outputPath2 = output.getAbsolutePath();
    output.delete();
    File tfFile = File.createTempFile("termfreq", "");
    Path tfPath = new Path(tfFile.getAbsolutePath());
    tfFile.delete();
    
    PCollection<String> docs = pipeline.readTextFile(input.getAbsolutePath());
    PTable<String, Pair<Long, Pair<String, Long>>>  joinedResults =
        generateTFIDF(docs, tfPath, typeFamily);
    pipeline.writeTextFile(joinedResults, outputPath1);
    PTable<String, Pair<Long, Pair<String, Long>>> uppercased = joinedResults.parallelDo(
        new MapKeysFn<String, String, Pair<Long, Pair<String, Long>>>() {
          @Override
          public String map(String k1) {
            return k1.toUpperCase();
          } 
        }, joinedResults.getPTableType());
    pipeline.writeTextFile(uppercased, outputPath2);
    pipeline.done();
    
    // Check the lowercase version...
    File outputFile = new File(outputPath1, "part-r-00000");
    outputFile.deleteOnExit();
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if ("this\t[2,[A,4]]".equals(line)) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
    
    // ...and the uppercase version
    outputFile = new File(outputPath2, "part-r-00000");
    outputFile.deleteOnExit();
    lines = Files.readLines(outputFile, Charset.defaultCharset());
    passed = false;
    for (String line : lines) {
      if ("THIS\t[2,[A,4]]".equals(line)) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }  
}
