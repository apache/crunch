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

import java.io.IOException;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 */
public class CancelJobsIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testRun() throws Exception {
    PipelineExecution pe = run();
    pe.waitUntilDone();
    PipelineResult pr = pe.getResult();
    assertEquals(PipelineExecution.Status.SUCCEEDED, pe.getStatus());
    assertEquals(2, pr.getStageResults().size());
  }

  @Test
  public void testGet() throws Exception {
    PipelineExecution pe = run();
    PipelineResult pr = pe.get();
    assertEquals(PipelineExecution.Status.SUCCEEDED, pe.getStatus());
    assertEquals(2, pr.getStageResults().size());
  }

  @Test
  public void testKill() throws Exception {
    PipelineExecution pe = run();
    pe.kill();
    pe.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, pe.getStatus());
  }

  @Test
  public void testKillGet() throws Exception {
    PipelineExecution pe = run();
    pe.kill();
    PipelineResult res = pe.get();
    assertFalse(res.succeeded());
    assertEquals(PipelineExecution.Status.KILLED, pe.getStatus());
  }

  @Test
  public void testCancelNoInterrupt() throws Exception {
    PipelineExecution pe = run();
    pe.cancel(false);
    pe.waitUntilDone();
    assertEquals(PipelineExecution.Status.SUCCEEDED, pe.getStatus());
  }

  @Test
  public void testCancelMayInterrupt() throws Exception {
    PipelineExecution pe = run();
    pe.cancel(true);
    pe.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, pe.getStatus());
  }

  @Test
  public void testKillMultipleTimes() throws Exception {
    PipelineExecution pe = run();
    for (int i = 0; i < 10; i++) {
      pe.kill();
    }
    pe.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, pe.getStatus());
  }

  @Test
  public void testKillAfterDone() throws Exception {
    PipelineExecution pe = run();
    pe.waitUntilDone();
    assertEquals(PipelineExecution.Status.SUCCEEDED, pe.getStatus());
    pe.kill(); // expect no-op
    assertEquals(PipelineExecution.Status.SUCCEEDED, pe.getStatus());
  }
  
  public PipelineExecution run() throws IOException {
    String shakes = tmpDir.copyResourceFileName("shakes.txt");
    String out = tmpDir.getFileName("cancel");
    Pipeline p = new MRPipeline(CancelJobsIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> words = p.readTextFile(shakes);
    p.write(words.count().top(20), To.textFile(out));
    return p.runAsync(); // need to hack to slow down job start up if this test becomes flaky.
  }
}
