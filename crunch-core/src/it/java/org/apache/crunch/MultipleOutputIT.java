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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.To;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
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

    PipelineResult result = run(new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration()),
        WritableTypeFamily.getInstance());

    // Ensure our multiple outputs were fused into a single job.
    assertEquals("parallel Dos not fused into a single job", 1, result.getStageResults().size());
  }

  @Test
  public void testCountersEnabled() throws IOException {
    PipelineResult result = run(new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration()),
        WritableTypeFamily.getInstance());
    
    assertEquals(1, result.getStageResults().size());
    StageResult stageResult = result.getStageResults().get(0);

    String counterGroup = CrunchOutputs.class.getName();
    assertEquals(3, stageResult.getCounterNames().get(counterGroup).size());
    assertEquals(1l, stageResult.getCounterValue(counterGroup, "out1"));
    assertEquals(1l, stageResult.getCounterValue(counterGroup, "out2"));
    assertEquals(0l, stageResult.getCounterValue(counterGroup, "out3"));
  }
  
  @Test
  public void testCountersDisabled() throws IOException {
    Configuration configuration = tmpDir.getDefaultConfiguration();
    configuration.setBoolean(CrunchOutputs.CRUNCH_DISABLE_OUTPUT_COUNTERS, true);
    
    PipelineResult result = run(new MRPipeline(MultipleOutputIT.class, configuration),
        WritableTypeFamily.getInstance());
    
    assertEquals(1, result.getStageResults().size());
    StageResult stageResult = result.getStageResults().get(0);
    
    assertFalse(stageResult.getCounterNames().containsKey(CrunchOutputs.CRUNCH_OUTPUTS));
  }
  
  
  public PipelineResult run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("letters.txt");
    String outputPathEven = tmpDir.getFileName("even");
    String outputPathOdd = tmpDir.getFileName("odd");
    String outputPathReduce = tmpDir.getFileName("reduce");
    
    PCollection<String> words = pipeline.read(At.textFile(inputPath, typeFamily.strings()));

    PCollection<String> evenCountWords = evenCountLetters(words, typeFamily);
    PCollection<String> oddCountWords = oddCountLetters(words, typeFamily);
    pipeline.writeTextFile(evenCountWords, outputPathEven);
    pipeline.writeTextFile(oddCountWords, outputPathOdd);

    evenCountWords.by(new FirstLetterFn(), typeFamily.strings())
        .groupByKey()
        .combineValues(Aggregators.<String>FIRST_N(10))
        .write(To.textFile(outputPathReduce));
    
    PipelineResult result = pipeline.done();

    checkFileContents(outputPathEven, Arrays.asList("bb"));
    checkFileContents(outputPathOdd, Arrays.asList("a"));
    checkNotEmpty(outputPathReduce);
    
    return result;
  }

  static class FirstLetterFn extends MapFn<String, String> {
    @Override
    public String map(String input) {
      return input.substring(0, 1);
    }
  }
  
  /**
   * Mutates the state of an input and then emits the mutated object.
   */
  static class AppendFn extends DoFn<StringWrapper, StringWrapper> {

    private String value;

    public AppendFn(String value) {
      this.value = value;
    }

    @Override
    public void process(StringWrapper input, Emitter<StringWrapper> emitter) {
      input.setValue(input.getValue() + value);
      emitter.emit(input);
    }

  }

  /**
   * Fusing multiple pipelines has a risk of running into object reuse bugs.
   * This test verifies that mutating the state of an object that is passed
   * through multiple streams of a pipeline doesn't allow one stream to affect
   * another.
   */
  @Test
  public void testFusedMappersObjectReuseBug() throws IOException {
    Pipeline pipeline = new MRPipeline(MultipleOutputIT.class, tmpDir.getDefaultConfiguration());
    PCollection<StringWrapper> stringWrappers = pipeline.readTextFile(tmpDir.copyResourceFileName("set2.txt"))
        .parallelDo(new StringWrapper.StringToStringWrapperMapFn(), Avros.reflects(StringWrapper.class));

    PCollection<String> stringsA = stringWrappers.parallelDo(new AppendFn("A"), stringWrappers.getPType())
        .parallelDo(new StringWrapper.StringWrapperToStringMapFn(), Writables.strings());
    PCollection<String> stringsB = stringWrappers.parallelDo(new AppendFn("B"), stringWrappers.getPType())
        .parallelDo(new StringWrapper.StringWrapperToStringMapFn(), Writables.strings());

    String outputA = tmpDir.getFileName("stringsA");
    String outputB = tmpDir.getFileName("stringsB");

    pipeline.writeTextFile(stringsA, outputA);
    pipeline.writeTextFile(stringsB, outputB);
    PipelineResult pipelineResult = pipeline.done();

    // Make sure fusing did actually occur
    assertEquals(1, pipelineResult.getStageResults().size());

    checkFileContents(outputA, Lists.newArrayList("cA", "dA", "aA"));
    checkFileContents(outputB, Lists.newArrayList("cB", "dB", "aB"));

  }

  private void checkNotEmpty(String filePath) throws IOException {
    File dir = new File(filePath);
    File[] partFiles = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("part");
      } 
    });
    assertTrue(partFiles.length > 0);
    assertTrue(Files.readLines(partFiles[0], Charset.defaultCharset()).size() > 0);
  }
  
  private void checkFileContents(String filePath, List<String> expected) throws IOException {
    File outputFile = new File(filePath, "part-m-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    assertEquals(expected, lines);
  }
}
