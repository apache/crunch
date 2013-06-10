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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchJobControl;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides APIs for job control at runtime to clients.
 *
 * This class has a thread that submits jobs when they become ready, monitors
 * the states of the running jobs, and updates the states of jobs based on the
 * state changes of their depending jobs states.
 *
 * It is thread-safe.
 */
public class MRExecutor implements MRPipelineExecution {

  private static final Log LOG = LogFactory.getLog(MRExecutor.class);

  private final CrunchJobControl control;
  private final Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private final Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize;
  private final CountDownLatch doneSignal = new CountDownLatch(1);
  private final CountDownLatch killSignal = new CountDownLatch(1);
  private final CappedExponentialCounter pollInterval;
  private AtomicReference<Status> status = new AtomicReference<Status>(Status.READY);
  private PipelineResult result;
  private Thread monitorThread;

  private String planDotFile;
  
  public MRExecutor(Class<?> jarClass, Map<PCollectionImpl<?>, Set<Target>> outputTargets,
      Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize) {
    this.control = new CrunchJobControl(jarClass.toString());
    this.outputTargets = outputTargets;
    this.toMaterialize = toMaterialize;
    this.monitorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        monitorLoop();
      }
    });
    this.pollInterval = isLocalMode()
      ? new CappedExponentialCounter(50, 1000)
      : new CappedExponentialCounter(500, 10000);
  }

  public void addJob(CrunchControlledJob job) {
    this.control.addJob(job);
  }

  public void setPlanDotFile(String planDotFile) {
    this.planDotFile = planDotFile;
  }
  
  public MRPipelineExecution execute() {
    monitorThread.start();
    return this;
  }

  /** Monitors running status. It is called in {@code MonitorThread}. */
  private void monitorLoop() {
    try {
      while (killSignal.getCount() > 0 && !control.allFinished()) {
        control.pollJobStatusAndStartNewOnes();
        killSignal.await(pollInterval.get(), TimeUnit.MILLISECONDS);
      }
      control.killAllRunningJobs();

      List<CrunchControlledJob> failures = control.getFailedJobList();
      if (!failures.isEmpty()) {
        System.err.println(failures.size() + " job failure(s) occurred:");
        for (CrunchControlledJob job : failures) {
          System.err.println(job.getJobName() + "(" + job.getJobID() + "): " + job.getMessage());
        }
      }
      List<PipelineResult.StageResult> stages = Lists.newArrayList();
      for (CrunchControlledJob job : control.getSuccessfulJobList()) {
        stages.add(new PipelineResult.StageResult(job.getJobName(), job.getJob().getCounters()));
      }

      for (PCollectionImpl<?> c : outputTargets.keySet()) {
        if (toMaterialize.containsKey(c)) {
          MaterializableIterable iter = toMaterialize.get(c);
          if (iter.isSourceTarget()) {
            iter.materialize();
            c.materializeAt((SourceTarget) iter.getSource());
          }
        } else {
          boolean materialized = false;
          for (Target t : outputTargets.get(c)) {
            if (!materialized) {
              if (t instanceof SourceTarget) {
                c.materializeAt((SourceTarget) t);
                materialized = true;
              } else {
                SourceTarget st = t.asSourceTarget(c.getPType());
                if (st != null) {
                  c.materializeAt(st);
                  materialized = true;
                }
              }
            }
          }
        }
      }

      synchronized (this) {
        if (killSignal.getCount() == 0) {
          status.set(Status.KILLED);
        } else if (!failures.isEmpty()) {
          status.set(Status.FAILED);
        } else {
          status.set(Status.SUCCEEDED);
        }
        result = new PipelineResult(stages, status.get());
      }
    } catch (InterruptedException e) {
      throw new AssertionError(e); // Nobody should interrupt us.
    } catch (IOException e) {
      LOG.error("Pipeline failed due to exception", e);
      status.set(Status.FAILED);
    } finally {
      doneSignal.countDown();
    }
  }

  @Override
  public String getPlanDotFile() {
    return planDotFile;
  }

  @Override
  public void waitFor(long timeout, TimeUnit timeUnit) throws InterruptedException {
    doneSignal.await(timeout, timeUnit);
  }

  @Override
  public void waitUntilDone() throws InterruptedException {
    doneSignal.await();
  }

  @Override
  public synchronized Status getStatus() {
    return status.get();
  }

  @Override
  public synchronized PipelineResult getResult() {
    return result;
  }

  @Override
  public void kill() throws InterruptedException {
    killSignal.countDown();
  }

  private static boolean isLocalMode() {
    Configuration conf = new Configuration();
    // Try to handle MapReduce version 0.20 or 0.22
    String jobTrackerAddress = conf.get("mapreduce.jobtracker.address",
        conf.get("mapred.job.tracker", "local"));
    return "local".equals(jobTrackerAddress);
  }

  @Override
  public List<MRJob> getJobs() {
    return Lists.transform(control.getAllJobs(), new Function<CrunchControlledJob, MRJob>() {
      @Override
      public MRJob apply(CrunchControlledJob job) {
        return job;
      }
    });
  }
}
