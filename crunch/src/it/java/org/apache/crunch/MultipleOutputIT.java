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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Files;

public class MultipleOutputIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  public static PCollection<String> evenCountLetters(PCollection<String> words, PTypeFamily typeFamily) {
    return words.parallelDo("even", new FilterFn<String>() {

      @Override
      public boolean accept(String input) {
        return input.length() % 2 == 0;
      }
    }, typeFamily.strings());
  }

  public static PCollection<String> oddCountLetters(PCollection<String> words, PTypeFamily typeFamily) {
    return words.parallelDo("odd", new FilterFn<String>() {

      @Override
      public boolean accept(String input) {
        return input.length() % 2 != 0;
      }
    }, typeFamily.strings());

  }

  public static PTable<String, Long> substr(PTable<String, Long> ptable) {
    return ptable.parallelDo(new DoFn<Pair<String, Long>, Pair<String, Long>>() {
      public void process(Pair<String, Long> input, Emitter<Pair<String, Long>> emitter) {
        if (input.first().length() > 0) {
          emitter.emit(Pair.of(input.first().substring(0, 1), input.second()));
        }
      }
    }, ptable.getPTableType());
  }

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance());
  }

  @Test
  public void testParallelDosFused() throws IOException {

    PipelineResult result = run(new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());

    // Ensure our multiple outputs were fused into a single job.
    assertEquals("parallel Dos not fused into a single job", 1, result.getStageResults().size());
  }

  public PipelineResult run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("letters.txt");
    String outputPathEven = tmpDir.getFileName("even");
    String outputPathOdd = tmpDir.getFileName("odd");

    PCollection<String> words = pipeline.read(At.textFile(inputPath, typeFamily.strings()));

    PCollection<String> evenCountWords = evenCountLetters(words, typeFamily);
    PCollection<String> oddCountWords = oddCountLetters(words, typeFamily);
    pipeline.writeTextFile(evenCountWords, outputPathEven);
    pipeline.writeTextFile(oddCountWords, outputPathOdd);

    PipelineResult result = pipeline.done();

    checkFileContents(outputPathEven, Arrays.asList("bb"));
    checkFileContents(outputPathOdd, Arrays.asList("a"));

    return result;
  }

  private void checkFileContents(String filePath, List<String> expected) throws IOException {
    File outputFile = new File(filePath, "part-m-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    assertEquals(expected, lines);
  }
}
