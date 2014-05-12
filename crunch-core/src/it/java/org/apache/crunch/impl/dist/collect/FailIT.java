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
package org.apache.crunch.impl.dist.collect;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FailIT extends CrunchTestSupport {

  static class InverseFn extends MapFn<String, Integer> {
    @Override
    public Integer map(String input) {
      int c = 0;
      return 1 / c;
    }
  };

  @Test
  public void testKill() throws Exception {
    Pipeline pipeline = new MRPipeline(FailIT.class, tempDir.getDefaultConfiguration());
    PCollection<String> p = pipeline.readTextFile(tempDir.copyResourceFileName("shakes.txt"));
    PCollection<Integer> result = p.parallelDo(new InverseFn(), Writables.ints());
    result.cache();

    PipelineExecution execution = pipeline.runAsync();

    while (!execution.isDone() && !execution.isCancelled()
        && execution.getStatus() != PipelineExecution.Status.FAILED
        && execution.getResult() == null) {
      try {
        Thread.sleep(1000);
        System.out.println("Job Status: " + execution.getStatus().toString());
      } catch (InterruptedException e) {
        System.err.println("ABORTING");
        e.printStackTrace();
        try {
          execution.kill();
          execution.waitUntilDone();
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
        throw new RuntimeException(e);
      }
    }
    System.out.println("Finished running job.");
    assertEquals(PipelineExecution.Status.FAILED, execution.getStatus());
  }

}
