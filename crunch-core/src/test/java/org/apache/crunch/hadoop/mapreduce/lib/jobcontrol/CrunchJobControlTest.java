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
package org.apache.crunch.hadoop.mapreduce.lib.jobcontrol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineCallable;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.plan.JobNameBuilder;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.To;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrunchJobControlTest {
  @Test
  public void testMaxRunningJobs() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt(RuntimeParameters.MAX_RUNNING_JOBS, 2);
    CrunchJobControl jobControl = new CrunchJobControl(conf, "group",
        ImmutableMap.<PipelineCallable<?>, Set<Target>>of());
    CrunchControlledJob job1 = createJob(1);
    CrunchControlledJob job2 = createJob(2);
    CrunchControlledJob job3 = createJob(3);

    // Submit job1 and job2.
    jobControl.addJob(job1);
    jobControl.addJob(job2);
    jobControl.pollJobStatusAndStartNewOnes();
    verify(job1).submit();
    verify(job2).submit();

    // Add job3 and expect it is pending.
    jobControl.addJob(job3);
    jobControl.pollJobStatusAndStartNewOnes();
    verify(job3, never()).submit();

    // Expect job3 is submitted after job1 is done.
    setSuccess(job1);
    jobControl.pollJobStatusAndStartNewOnes();
    verify(job3).submit();
  }

  private static class IncrementingPipelineCallable extends PipelineCallable<Void> {

    private String name;
    private boolean executed;

    public IncrementingPipelineCallable(String name) {
      this.name = name;
    }

    @Override
    public Status call() {
      executed = true;
      return Status.SUCCESS;
    }

    public boolean isExecuted() { return executed; }

    @Override
    public Void getOutput(Pipeline pipeline) {
      return null;
    }
  }

  @Test
  public void testSequentialDo() throws IOException, InterruptedException {
    Target t1 = To.textFile("foo");
    Target t2 = To.textFile("bar");
    Target t3 = To.textFile("baz");
    IncrementingPipelineCallable first = new IncrementingPipelineCallable("first");
    IncrementingPipelineCallable second = new IncrementingPipelineCallable("second");
    IncrementingPipelineCallable third = new IncrementingPipelineCallable("third");
    CrunchControlledJob job1 = createJob(1, ImmutableSet.of(t1));
    CrunchControlledJob job2 = createJob(2, ImmutableSet.of(t2));
    CrunchControlledJob job3 = createJob(3, ImmutableSet.of(t3));
    Configuration conf = new Configuration();
    Map<PipelineCallable<?>, Set<Target>> pipelineCallables = Maps.newHashMap();
    pipelineCallables.put(first, ImmutableSet.<Target>of());
    pipelineCallables.put(second, ImmutableSet.<Target>of(t1));
    pipelineCallables.put(third, ImmutableSet.<Target>of(t2, t3));
    CrunchJobControl jobControl = new CrunchJobControl(conf, "group", pipelineCallables);

    jobControl.addJob(job1);
    jobControl.addJob(job2);
    jobControl.addJob(job3);
    jobControl.pollJobStatusAndStartNewOnes();
    verify(job1).submit();
    verify(job2).submit();
    verify(job3).submit();
    assertTrue(first.isExecuted());

    setSuccess(job1);
    jobControl.pollJobStatusAndStartNewOnes();
    assertTrue(second.isExecuted());

    setSuccess(job2);
    jobControl.pollJobStatusAndStartNewOnes();
    assertFalse(third.isExecuted());

    setSuccess(job3);
    jobControl.pollJobStatusAndStartNewOnes();
    assertTrue(third.isExecuted());
  }

  private CrunchControlledJob createJob(int jobID) {
    return createJob(jobID, ImmutableSet.<Target>of());
  }

  private CrunchControlledJob createJob(int jobID, Set<Target> targets) {
    Job mrJob = mock(Job.class);
    when(mrJob.getConfiguration()).thenReturn(new Configuration());
    CrunchControlledJob job = new CrunchControlledJob(
        jobID,
        mrJob,
        new JobNameBuilder(mrJob.getConfiguration(), "test", 1, 1),
        targets,
        mock(CrunchControlledJob.Hook.class),
        mock(CrunchControlledJob.Hook.class));
    return spy(job);
  }

  private void setSuccess(CrunchControlledJob job) throws IOException, InterruptedException {
    when(job.checkState()).thenReturn(MRJob.State.SUCCESS);
    when(job.getJobState()).thenReturn(MRJob.State.SUCCESS);
  }
}
