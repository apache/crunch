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

import java.util.concurrent.CancellationException;

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
    run(false);
  }
  
  @Test
  public void testCancel() throws Exception {
    run(true);
  }
  
  public void run(boolean cancel) throws Exception {
    String shakes = tmpDir.copyResourceFileName("shakes.txt");
    String out = tmpDir.getFileName("cancel");
    Pipeline p = new MRPipeline(CancelJobsIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> words = p.readTextFile(shakes);
    p.write(words.count().top(20), To.textFile(out));
    PipelineExecution pe = p.runAsync();
    if (cancel) {
      boolean cancelled = false;
      pe.cancel(true);
      try {
        pe.get();
      } catch (CancellationException e) {
        cancelled = true;
      }
      assertTrue(cancelled);
    } else {
      PipelineResult pr = pe.get();
      assertEquals(2, pr.getStageResults().size());
    }
  }
}
