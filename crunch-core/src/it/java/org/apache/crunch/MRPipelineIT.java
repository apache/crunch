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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.Map;

import com.google.common.io.Files;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.To;
import org.apache.crunch.io.text.TextFileTarget;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Rule;
import org.junit.Test;

public class MRPipelineIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void materializedColShouldBeWritten() throws Exception {
    File textFile = tmpDir.copyResourceFile("shakes.txt");
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> genericCollection = pipeline.readTextFile(textFile.getAbsolutePath());
    pipeline.run();
    PCollection<String> filter = genericCollection.filter("Filtering data", FilterFns.<String>ACCEPT_ALL());
    filter.materialize();
    pipeline.run();
    File file = tmpDir.getFile("output.txt");
    Target outFile = To.textFile(file.getAbsolutePath());
    PCollection<String> write = filter.write(outFile);
    write.materialize();
    pipeline.run();
  }
  
  
  
  @Test
  public void testPGroupedTableToMultipleOutputs() throws IOException{
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    PGroupedTable<String, String> groupedLineTable = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt")).by(IdentityFn.<String>getInstance(), Writables.strings()).groupByKey();
    
    PTable<String, String> ungroupedTableA = groupedLineTable.ungroup();
    PTable<String, String> ungroupedTableB = groupedLineTable.ungroup();
    
    File outputDirA = tmpDir.getFile("output_a");
    File outputDirB = tmpDir.getFile("output_b");
    
    pipeline.writeTextFile(ungroupedTableA, outputDirA.getAbsolutePath());
    pipeline.writeTextFile(ungroupedTableB, outputDirB.getAbsolutePath());
    PipelineResult result = pipeline.done();
    for(StageResult stageResult : result.getStageResults()){
      assertTrue(stageResult.getStageName().length() > 1);
      assertTrue(stageResult.getStageId().length() > 1);
    }

    // Verify that output from a single PGroupedTable can be sent to multiple collections
    assertTrue(new File(outputDirA, "part-r-00000").exists());
    assertTrue(new File(outputDirB, "part-r-00000").exists());
  }
 
  @Test
  public void testWritingOfDotfile() throws IOException {
    File dotfileDir = Files.createTempDir();
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    pipeline.getConfiguration().set(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR, dotfileDir.getAbsolutePath());

    PCollection<String> lines = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));
    pipeline.write(
        lines.parallelDo(IdentityFn.<String>getInstance(), Writables.strings()),
        To.textFile(tmpDir.getFile("output").getAbsolutePath()));
    pipeline.done();

    File[] files = dotfileDir.listFiles((FileFilter)new SuffixFileFilter(".dot"));
    assertEquals(1, files.length);
    String fileName = files[0].getName();
    String fileNamePrefix = URLEncoder.encode(pipeline.getName(), "UTF-8");
    fileNamePrefix = (fileNamePrefix.length() < 150) ? fileNamePrefix : fileNamePrefix.substring(0, 150);
    assertTrue("DOT file name '" + fileName + "' did not start with the pipeline name '" + fileNamePrefix + "'.",
        fileName.startsWith(fileNamePrefix));
    
    String regex = ".*_\\d{4}-\\d{2}-\\d{2}_\\d{2}\\.\\d{2}\\.\\d{2}\\.\\d{3}_jobplan\\.dot";
    assertTrue("DOT file name '" + fileName + "' did not match regex '" + regex + "'.", fileName.matches(regex));
  }

  @Test
  public void testJobCredentials() throws IOException {
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> lines = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"));

    pipeline.write(lines, new SecretTextFileTarget(tmpDir.getFile("output").getAbsolutePath()));

    PipelineResult pipelineResult = pipeline.done();
    assertTrue(pipelineResult.succeeded());
  }

  private static class SecretTextFileTarget extends TextFileTarget {
    public SecretTextFileTarget(String path) {
      super(path);
    }

    @Override
    public Target outputConf(String key, String value) {
      return super.outputConf(key, value);
    }

    @Override
    public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
      Converter converter = ptype.getConverter();
      Class keyClass = converter.getKeyClass();
      Class valueClass = converter.getValueClass();
      FormatBundle fb = FormatBundle.forOutput(SecretTextOutputFormat.class);
      configureForMapReduce(job, keyClass, valueClass, fb, outputPath, name);

      job.getCredentials().addSecretKey(new Text("secret"), "myPassword".getBytes());
    }
  }
  private static class SecretTextOutputFormat extends TextOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      byte[] secret = job.getCredentials().getSecretKey(new Text("secret"));
      assertEquals("job credentials did not match", "myPassword", new String(secret));
      return super.getRecordWriter(job);
    }
  }
}
