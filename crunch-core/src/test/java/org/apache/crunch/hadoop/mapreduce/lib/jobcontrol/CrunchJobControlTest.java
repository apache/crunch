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

import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;

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
    CrunchJobControl jobControl = new CrunchJobControl(conf, "group");
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

  private CrunchControlledJob createJob(int jobID) throws IOException, InterruptedException {
    Job mrJob = mock(Job.class);
    when(mrJob.getConfiguration()).thenReturn(new Configuration());
    CrunchControlledJob job = new CrunchControlledJob(
        jobID,
        mrJob,
        mock(CrunchControlledJob.Hook.class),
        mock(CrunchControlledJob.Hook.class));
    return spy(job);
  }

  private void setSuccess(CrunchControlledJob job) throws IOException, InterruptedException {
    when(job.checkState()).thenReturn(MRJob.State.SUCCESS);
    when(job.getJobState()).thenReturn(MRJob.State.SUCCESS);
  }
}
