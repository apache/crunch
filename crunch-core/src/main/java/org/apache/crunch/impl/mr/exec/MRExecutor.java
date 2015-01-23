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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;

import org.apache.crunch.PipelineCallable;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchJobControl;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
public class MRExecutor extends AbstractFuture<PipelineResult> implements MRPipelineExecution {

  private static final Logger LOG = LoggerFactory.getLogger(MRExecutor.class);

  private final CrunchJobControl control;
  private final Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private final Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize;
  private final Set<Target> appendedTargets;
  private final CountDownLatch doneSignal = new CountDownLatch(1);
  private final CountDownLatch killSignal = new CountDownLatch(1);
  private final CappedExponentialCounter pollInterval;
  private AtomicReference<Status> status = new AtomicReference<Status>(Status.READY);
  private PipelineResult result;
  private Thread monitorThread;
  private boolean started;

  private Map<String, String> namedDotFiles;
  
  public MRExecutor(
      Configuration conf,
      Class<?> jarClass,
      Map<PCollectionImpl<?>, Set<Target>> outputTargets,
      Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize,
      Set<Target> appendedTargets,
      Map<PipelineCallable<?>, Set<Target>> pipelineCallables) {
    this.control = new CrunchJobControl(conf, jarClass.toString(), pipelineCallables);
    this.outputTargets = outputTargets;
    this.toMaterialize = toMaterialize;
    this.appendedTargets = appendedTargets;
    this.monitorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        monitorLoop();
      }
    });
    this.pollInterval = isLocalMode()
      ? new CappedExponentialCounter(50, 1000)
      : new CappedExponentialCounter(500, 10000);

    this.namedDotFiles = new ConcurrentHashMap<String, String>();
  }

  public void addJob(CrunchControlledJob job) {
    this.control.addJob(job);
  }

  public void addNamedDotFile(String fileName, String planDotFile) {
    this.namedDotFiles.put(fileName, planDotFile);
  }

  @Override
  public String getPlanDotFile() {
    return this.namedDotFiles.get("jobplan");
  }
  
  @Override
  public Map<String, String> getNamedDotFiles() {
    return ImmutableMap.copyOf(this.namedDotFiles);
  }

  public synchronized MRPipelineExecution execute() {
    if (!started) {
      monitorThread.start();
      started = true;
    }
    return this;
  }

  /** Monitors running status. It is called in {@code MonitorThread}. */
  private void monitorLoop() {
    status.set(Status.RUNNING);
    try {
      while (killSignal.getCount() > 0 && !control.allFinished() && !control.anyFailures()) {
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
      List<PipelineCallable<?>> failedCallables = control.getFailedCallables();
      if (!failedCallables.isEmpty()) {
        System.err.println(failedCallables.size() + " callable failure(s) occurred:");
        for (PipelineCallable<?> c : failedCallables) {
          System.err.println(c.getName() + ": " + c.getMessage());
        }
      }
      boolean hasFailures = !failures.isEmpty() || !failedCallables.isEmpty();
      List<PipelineResult.StageResult> stages = Lists.newArrayList();
      for (CrunchControlledJob job : control.getSuccessfulJobList()) {
        stages.add(new PipelineResult.StageResult(job.getJobName(), job.getMapredJobID().toString(), job.getCounters(),
            job.getStartTimeMsec(), job.getJobStartTimeMsec(), job.getJobEndTimeMsec(), job.getEndTimeMsec()));
      }

      if (!hasFailures) {
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
              if (!materialized && !appendedTargets.contains(t)) {
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
      }

      synchronized (this) {
        if (killSignal.getCount() == 0) {
          status.set(Status.KILLED);
        } else if (!failures.isEmpty() || !failedCallables.isEmpty()) {
          status.set(Status.FAILED);
        } else {
          status.set(Status.SUCCEEDED);
        }
        result = new PipelineResult(stages, status.get());
        set(result);
      }
    } catch (InterruptedException e) {
      throw new AssertionError(e); // Nobody should interrupt us.
    } catch (Exception e) {
      LOG.error("Pipeline failed due to exception", e);
      status.set(Status.FAILED);
      setException(e);
    } finally {
      doneSignal.countDown();
    }
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
  public PipelineResult get() throws InterruptedException, ExecutionException {
    if (getStatus() == Status.READY) {
      execute();
    }
    return super.get();
  }

  @Override
  public PipelineResult get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException,
      ExecutionException {
    if (getStatus() == Status.READY) {
      execute();
    }
    return super.get(timeout, unit);
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

  @Override
  protected void interruptTask() {
    try {
      kill();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isLocalMode() {
    Configuration conf = new Configuration();
    String frameworkName = conf.get("mapreduce.framework.name", "");
    if (frameworkName.isEmpty()) {
      // Fallback to older jobtracker-based checks
      frameworkName = conf.get("mapreduce.jobtracker.address",
          conf.get("mapred.job.tracker", "local"));
    }
    return "local".equals(frameworkName);
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
