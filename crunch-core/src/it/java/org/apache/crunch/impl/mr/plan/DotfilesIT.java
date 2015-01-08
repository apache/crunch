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
package org.apache.crunch.impl.mr.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Files;

public class DotfilesIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Rule
  public TemporaryPath dotfileDir = TemporaryPaths.create();

  enum WordCountStats {
    ANDS
  }

  public static PTable<String, Long> wordCount(PCollection<String> words, PTypeFamily typeFamily) {
    return Aggregate.count(words.parallelDo(new DoFn<String, String>() {

      @Override
      public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
          if ("and".equals(word)) {
            increment(WordCountStats.ANDS);
          }
        }
      }
    }, typeFamily.strings()));
  }

  @Test
  public void testPlanDotfileWithOutputDir() throws Throwable {

    Configuration conf = tmpDir.getDefaultConfiguration();

    DotfileUtills.setPipelineDotfileOutputDir(conf, dotfileDir.getRootFileName());

    run(new MRPipeline(DotfilesIT.class, conf), WritableTypeFamily.getInstance());

    String[] dotfileNames = dotfileNames(dotfileDir.getRootFile());

    assertEquals(1, dotfileNames.length);

    assertTrue(containsFileEndingWith(dotfileNames, "jobplan.dot"));

    assertTrue("PlanDotfile should always be present in the Configuration",
        conf.get(PlanningParameters.PIPELINE_PLAN_DOTFILE).length() > 0);
  }

  @Test
  public void testPlanDotfileWithoutOutputDir() throws Throwable {

    Configuration conf = tmpDir.getDefaultConfiguration();

    run(new MRPipeline(DotfilesIT.class, conf), WritableTypeFamily.getInstance());

    String[] dotfileNames = dotfileNames(dotfileDir.getRootFile());

    assertEquals(0, dotfileNames.length);
    assertTrue("PlanDotfile should always be present in the Configuration",
        conf.get(PlanningParameters.PIPELINE_PLAN_DOTFILE).length() > 0);
  }

  @Test
  public void testDebugDotfiles() throws Throwable {

    Configuration conf = tmpDir.getDefaultConfiguration();

    DotfileUtills.setPipelineDotfileOutputDir(conf, dotfileDir.getRootFileName());
    DotfileUtills.enableDebugDotfiles(conf);

    run(new MRPipeline(DotfilesIT.class, conf), WritableTypeFamily.getInstance());

    String[] dotfileNames = dotfileNames(dotfileDir.getRootFile());

    assertEquals(5, dotfileNames.length);

    assertTrue(containsFileEndingWith(dotfileNames, "jobplan.dot"));
    assertTrue(containsFileEndingWith(dotfileNames, "split_graph_plan.dot"));
    assertTrue(containsFileEndingWith(dotfileNames, "rt_plan.dot"));
    assertTrue(containsFileEndingWith(dotfileNames, "base_graph_plan.dot"));
    assertTrue(containsFileEndingWith(dotfileNames, "lineage_plan.dot"));

    assertTrue("PlanDotfile should always be present in the Configuration",
        conf.get(PlanningParameters.PIPELINE_PLAN_DOTFILE).length() > 0);
  }

  @Test
  public void testDebugDotfilesEnabledButNoOutputDirSet() throws Throwable {

    Configuration conf = tmpDir.getDefaultConfiguration();

    DotfileUtills.enableDebugDotfiles(conf);

    run(new MRPipeline(DotfilesIT.class, conf), WritableTypeFamily.getInstance());

    String[] dotfileNames = dotfileNames(dotfileDir.getRootFile());

    assertEquals(0, dotfileNames.length);

    assertTrue("PlanDotfile should always be present in the Configuration",
        conf.get(PlanningParameters.PIPELINE_PLAN_DOTFILE).length() > 0);
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");
    String outputPath = tmpDir.getFileName("output");

    PCollection<String> shakespeare = pipeline.read(At.textFile(inputPath, typeFamily.strings()));
    PTable<String, Long> wordCount = wordCount(shakespeare, typeFamily);

    pipeline.writeTextFile(wordCount, outputPath);

    PipelineResult res = pipeline.done();
    assertTrue(res.succeeded());
    List<PipelineResult.StageResult> stageResults = res.getStageResults();

    assertEquals(1, stageResults.size());
    assertEquals(427, stageResults.get(0).getCounterValue(WordCountStats.ANDS));

    File outputFile = new File(outputPath, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if (line.startsWith("Macbeth\t28") || line.startsWith("[Macbeth,28]")) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }

  private boolean containsFileEndingWith(String[] fileNames, String suffix) {
    for (String fn : fileNames) {
      if (fn.endsWith(suffix))
        return true;
    }
    return false;
  }

  private String[] dotfileNames(File rootDir) {

    File[] dotfileFiles = rootDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".dot");
      }
    });

    String[] fileNames = new String[dotfileFiles.length];
    int i = 0;
    for (File file : dotfileFiles) {
      fileNames[i++] = file.getName();
    }

    return fileNames;
  }
}
