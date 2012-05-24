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
import static org.junit.Assert.assertEquals;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.io.To;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class MultipleOutputTest {
  
  public static PCollection<String> evenCountLetters(PCollection<String> words, PTypeFamily typeFamily) {
    return words.parallelDo("even", new FilterFn<String>(){

        @Override
        public boolean accept(String input) {
            return input.length() % 2 == 0;
        }}, typeFamily.strings());
  }
  
  public static PCollection<String> oddCountLetters(PCollection<String> words, PTypeFamily typeFamily) {
      return words.parallelDo("odd", new FilterFn<String>(){

        @Override
        public boolean accept(String input) {
            return input.length() % 2 != 0;
        }}, typeFamily.strings());
       
    }
  
  public static PTable<String, Long> substr(PTable<String, Long> ptable) {
	return ptable.parallelDo(new DoFn<Pair<String, Long>, Pair<String, Long>>() {
	  public void process(Pair<String, Long> input,
		  Emitter<Pair<String, Long>> emitter) {
		if (input.first().length() > 0) {
		  emitter.emit(Pair.of(input.first().substring(0, 1), input.second()));
		}
	  }      
    }, ptable.getPTableType());
  }
  
  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(MultipleOutputTest.class), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(MultipleOutputTest.class), AvroTypeFamily.getInstance());
  }
 
  
  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
	String inputPath = FileHelper.createTempCopyOf("letters.txt");
	File outputEven = FileHelper.createOutputPath();
	File outputOdd = FileHelper.createOutputPath();
	String outputPathEven = outputEven.getAbsolutePath();
	String outputPathOdd = outputOdd.getAbsolutePath();
	
    PCollection<String> words = pipeline.read(
         At.textFile(inputPath, typeFamily.strings()));
    
    PCollection<String> evenCountWords = evenCountLetters(words, typeFamily);
    PCollection<String> oddCountWords = oddCountLetters(words, typeFamily);
    pipeline.writeTextFile(evenCountWords, outputPathEven);
    pipeline.writeTextFile(oddCountWords, outputPathOdd);
    
    pipeline.done();
   
    checkFileContents(outputPathEven, Arrays.asList("bb"));
    checkFileContents(outputPathOdd, Arrays.asList("a"));
   
	outputEven.deleteOnExit();
	outputOdd.deleteOnExit();
  }  
  
  private void checkFileContents(String filePath, List<String> expected) throws IOException{
    File outputFile = new File(filePath, "part-m-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    assertEquals(expected, lines);
  }
}
