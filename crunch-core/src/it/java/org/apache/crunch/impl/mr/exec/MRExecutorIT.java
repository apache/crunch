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
package org.apache.crunch.impl.mr.exec;

import org.apache.commons.lang.time.StopWatch;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.apache.crunch.types.writable.Writables.longs;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MRExecutorIT {

  private static class SleepForeverFn extends DoFn<Long, Long> {
    @Override
    public void process(Long input, Emitter<Long> emitter) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  /**
   * Tests that the pipeline should be stopped immediately when one of the jobs
   * get failed. The rest of running jobs should be killed.
   */
  @Test
  public void testStopPipelineImmediatelyOnJobFailure() throws Exception {
    String inPath = tmpDir.copyResourceFileName("shakes.txt");
    MRPipeline pipeline = new MRPipeline(MRExecutorIT.class);

    // Issue two jobs that sleep forever.
    PCollection<String> in = pipeline.read(From.textFile(inPath));
    for (int i = 0; i < 2; i++) {
      in.count()
          .values()
          .parallelDo(new SleepForeverFn(), longs())
          .write(To.textFile(tmpDir.getPath("out_" + i)));
    }
    MRPipelineExecution exec = pipeline.runAsync();

    // Wait until both of the two jobs are submitted.
    List<MRJob> jobs = exec.getJobs();
    assertEquals(2, jobs.size());
    StopWatch watch = new StopWatch();
    watch.start();
    int numOfJobsSubmitted = 0;
    while (numOfJobsSubmitted < 2 && watch.getTime() < 10000) {
      numOfJobsSubmitted = 0;
      for (MRJob job : jobs) {
        if (job.getJobState() == MRJob.State.RUNNING) {
          numOfJobsSubmitted++;
        }
      }
      Thread.sleep(100);
    }
    assertEquals(2, numOfJobsSubmitted);

    // Kill one of them.
    Job job0 = jobs.get(0).getJob();
    job0.killJob();

    // Expect the pipeline exits and the other job is killed.
    StopWatch watch2 = new StopWatch();
    watch2.start();
    Job job1 = jobs.get(1).getJob();
    while (!job1.isComplete() && watch2.getTime() < 10000) {
      Thread.sleep(100);
    }
    assertTrue(job1.isComplete());
    assertEquals(PipelineExecution.Status.FAILED, exec.getStatus());
  }
}
