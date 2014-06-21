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

import com.google.common.collect.ImmutableMap;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;

public class PipelineCallableIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMRShakes() throws Exception {
    run(new MRPipeline(PipelineCallableIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("shakes.txt"), false /* fail */);
  }

  @Test
  public void testFailure() throws Exception {
    run(new MRPipeline(PipelineCallableIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("shakes.txt"), true /* fail */);
  }

  public static int INC1 = 0;
  public static int INC2 = 0;

  public static void run(Pipeline p, final String input, final boolean fail) {

    PTable<String, Long> top3 = p.sequentialDo(new PipelineCallable<PCollection<String>>() {
      @Override
      public Status call() {
        INC1 = 17;
        return fail ? Status.FAILURE : Status.SUCCESS;
      }

      @Override
      public PCollection<String> getOutput(Pipeline pipeline) {
        return pipeline.readTextFile(input);
      }
    }.named("first"))
    .sequentialDo("onInput", new PipelineCallable<PCollection<String>>() {
      @Override
      protected PCollection<String> getOutput(Pipeline pipeline) {
        return getOnlyPCollection();
      }

      @Override
      public Status call() throws Exception {
        return Status.SUCCESS;
      }
    })
    .count()
    .sequentialDo("label", new PipelineCallable<PTable<String, Long>>() {
      @Override
      public Status call() {
        INC2 = 29;
        if (getPCollection("label") != null) {
          return Status.SUCCESS;
        }
        return Status.FAILURE;
      }

      @Override
      public PTable<String, Long> getOutput(Pipeline pipeline) {
        return (PTable<String, Long>) getOnlyPCollection();
      }
    }.named("second"))
    .top(3);

    if (fail) {
      assertFalse(p.run().succeeded());
    } else {
      Map<String, Long> counts = top3.materializeToMap();
      assertEquals(ImmutableMap.of("", 788L, "Enter Macbeth.", 7L, "Exeunt.", 21L), counts);
      assertEquals(17, INC1);
      assertEquals(29, INC2);
    }
    p.done();
  }
}
