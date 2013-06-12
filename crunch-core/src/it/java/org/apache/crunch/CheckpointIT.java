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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

public class CheckpointIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test
  public void testCheckpoints() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");
    Pipeline p = new MRPipeline(CheckpointIT.class);
    String inter = tmpDir.getFileName("intermediate");
    PipelineResult one = run(p, tmpDir, inputPath, inter, false);
    assertTrue(one.succeeded());
    assertEquals(2, one.getStageResults().size());
    PipelineResult two = run(p, tmpDir, inputPath, inter, false);
    assertTrue(two.succeeded());
    assertEquals(1, two.getStageResults().size());
  }
  
  @Test
  public void testUnsuccessfulCheckpoint() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");
    Pipeline p = new MRPipeline(CheckpointIT.class);
    String inter = tmpDir.getFileName("intermediate");
    PipelineResult one = run(p, tmpDir, inputPath, inter, true);
    assertFalse(one.succeeded());
    PipelineResult two = run(p, tmpDir, inputPath, inter, false);
    assertTrue(two.succeeded());
    assertEquals(2, two.getStageResults().size());
  }
  
  @Test
  public void testModifiedFileCheckpoint() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");
    Pipeline p = new MRPipeline(CheckpointIT.class);
    Path inter = tmpDir.getPath("intermediate");
    PipelineResult one = run(p, tmpDir, inputPath, inter.toString(), false);
    assertTrue(one.succeeded());
    assertEquals(2, one.getStageResults().size());
    // Update the input path
    inputPath = tmpDir.copyResourceFileName("shakes.txt");
    PipelineResult two = run(p, tmpDir, inputPath, inter.toString(), false);
    assertTrue(two.succeeded());
    assertEquals(2, two.getStageResults().size());
  }
  
  public static PipelineResult run(Pipeline pipeline, TemporaryPath tmpDir, 
      String shakesInputPath, String intermediatePath,
      final boolean fail)
      throws Exception {
    PCollection<String> shakes = pipeline.readTextFile(shakesInputPath);
    PTable<String, Long> cnts = shakes.parallelDo("split words", new DoFn<String, String>() {
      @Override
      public void process(String line, Emitter<String> emitter) {
        if (fail) {
          throw new RuntimeException("Failure!");
        }
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
        }
      }
    }, Avros.strings()).count();
    cnts.write(To.avroFile(intermediatePath), WriteMode.CHECKPOINT);
    PTable<String, Long> singleCounts = cnts.keys().count();
    singleCounts.write(To.textFile(tmpDir.getFileName("singleCounts")), WriteMode.OVERWRITE);
    return pipeline.run();
  }
}
