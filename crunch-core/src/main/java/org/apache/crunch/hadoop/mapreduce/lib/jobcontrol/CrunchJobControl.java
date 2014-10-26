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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.crunch.PipelineCallable;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRJob.State;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates a set of MapReduce jobs and its dependency.
 * 
 * It tracks the states of the jobs by placing them into different tables
 * according to their states.
 * 
 * This class provides APIs for the client app to add a job to the group and to
 * get the jobs in the group in different states. When a job is added, an ID
 * unique to the group is assigned to the job.
 */
public class CrunchJobControl {

  private Map<Integer, CrunchControlledJob> waitingJobs;
  private Map<Integer, CrunchControlledJob> readyJobs;
  private Map<Integer, CrunchControlledJob> runningJobs;
  private Map<Integer, CrunchControlledJob> successfulJobs;
  private Map<Integer, CrunchControlledJob> failedJobs;
  private Map<PipelineCallable<?>, Set<Target>> allPipelineCallables;
  private Set<PipelineCallable<?>> activePipelineCallables;
  private List<PipelineCallable<?>> failedCallables;

  private Logger log = LoggerFactory.getLogger(CrunchJobControl.class);

  private final String groupName;
  private final int maxRunningJobs;
  private int jobSequence = 1;

  /**
   * Construct a job control for a group of jobs.
   * 
   * @param groupName
   *          a name identifying this group
   */
  public CrunchJobControl(Configuration conf, String groupName,
                          Map<PipelineCallable<?>, Set<Target>> pipelineCallables) {
    this.waitingJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.readyJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.runningJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.successfulJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.failedJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.groupName = groupName;
    this.maxRunningJobs = conf.getInt(RuntimeParameters.MAX_RUNNING_JOBS, 5);
    this.allPipelineCallables = pipelineCallables;
    this.activePipelineCallables = allPipelineCallables.keySet();
    this.failedCallables = Lists.newArrayList();
  }

  private static List<CrunchControlledJob> toList(Map<Integer, CrunchControlledJob> jobs) {
    ArrayList<CrunchControlledJob> retv = new ArrayList<CrunchControlledJob>();
    synchronized (jobs) {
      for (CrunchControlledJob job : jobs.values()) {
        retv.add(job);
      }
    }
    return retv;
  }

  /**
   * @return the jobs in the waiting state
   */
  public List<CrunchControlledJob> getWaitingJobList() {
    return toList(this.waitingJobs);
  }

  /**
   * @return the jobs in the running state
   */
  public List<CrunchControlledJob> getRunningJobList() {
    return toList(this.runningJobs);
  }

  /**
   * @return the jobs in the ready state
   */
  public List<CrunchControlledJob> getReadyJobsList() {
    return toList(this.readyJobs);
  }

  /**
   * @return the jobs in the success state
   */
  public List<CrunchControlledJob> getSuccessfulJobList() {
    return toList(this.successfulJobs);
  }

  public List<CrunchControlledJob> getFailedJobList() {
    return toList(this.failedJobs);
  }

  /**
   * @return the jobs in all states
   */
  public synchronized List<CrunchControlledJob> getAllJobs() {
    return ImmutableList.<CrunchControlledJob>builder()
        .addAll(waitingJobs.values())
        .addAll(readyJobs.values())
        .addAll(runningJobs.values())
        .addAll(successfulJobs.values())
        .addAll(failedJobs.values())
        .build();
  }

  private static void addToQueue(CrunchControlledJob aJob,
      Map<Integer, CrunchControlledJob> queue) {
    synchronized (queue) {
      queue.put(aJob.getJobID(), aJob);
    }
  }

  private void addToQueue(CrunchControlledJob aJob) {
    Map<Integer, CrunchControlledJob> queue = getQueue(aJob.getJobState());
    addToQueue(aJob, queue);
  }

  private Map<Integer, CrunchControlledJob> getQueue(State state) {
    Map<Integer, CrunchControlledJob> retv = null;
    if (state == State.WAITING) {
      retv = this.waitingJobs;
    } else if (state == State.READY) {
      retv = this.readyJobs;
    } else if (state == State.RUNNING) {
      retv = this.runningJobs;
    } else if (state == State.SUCCESS) {
      retv = this.successfulJobs;
    } else if (state == State.FAILED || state == State.DEPENDENT_FAILED) {
      retv = this.failedJobs;
    }
    return retv;
  }

  /**
   * Add a new job.
   * 
   * @param aJob
   *          the new job
   */
  synchronized public void addJob(CrunchControlledJob aJob) {
    aJob.setJobState(State.WAITING);
    this.addToQueue(aJob);
  }

  synchronized private void checkRunningJobs() throws IOException,
      InterruptedException {

    Map<Integer, CrunchControlledJob> oldJobs = null;
    oldJobs = this.runningJobs;
    this.runningJobs = new Hashtable<Integer, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  synchronized private void checkWaitingJobs() throws IOException,
      InterruptedException {
    Map<Integer, CrunchControlledJob> oldJobs = null;
    oldJobs = this.waitingJobs;
    this.waitingJobs = new Hashtable<Integer, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  private Set<Target> getUnfinishedTargets() {
    Set<Target> unfinished = Sets.newHashSet();
    for (CrunchControlledJob job : runningJobs.values()) {
      unfinished.addAll(job.getAllTargets());
    }
    for (CrunchControlledJob job : readyJobs.values()) {
      unfinished.addAll(job.getAllTargets());
    }
    for (CrunchControlledJob job : waitingJobs.values()) {
      unfinished.addAll(job.getAllTargets());
    }
    return unfinished;
  }

  synchronized private void executeReadySeqDoFns() {
    Set<Target> unfinished = getUnfinishedTargets();
    Set<PipelineCallable<?>> oldPipelineCallables = activePipelineCallables;
    this.activePipelineCallables = Sets.newHashSet();
    List<Callable<PipelineCallable.Status>> callablesToRun = Lists.newArrayList();
    for (final PipelineCallable<?> pipelineCallable : oldPipelineCallables) {
      if (Sets.intersection(allPipelineCallables.get(pipelineCallable), unfinished).isEmpty()) {
        if (pipelineCallable.runSingleThreaded()) {
          try {
            if (pipelineCallable.call() != PipelineCallable.Status.SUCCESS) {
              failedCallables.add(pipelineCallable);
            }
          } catch (Throwable t) {
            pipelineCallable.setMessage(t.getLocalizedMessage());
            failedCallables.add(pipelineCallable);
          }
        } else {
          callablesToRun.add(pipelineCallable);
        }
      } else {
        // Still need to run this one
       activePipelineCallables.add(pipelineCallable);
      }
    }

    ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    try {
      List<Future<PipelineCallable.Status>> res = es.invokeAll(callablesToRun);
      for (int i = 0; i < res.size(); i++) {
        if (res.get(i).get() != PipelineCallable.Status.SUCCESS) {
          failedCallables.add((PipelineCallable) callablesToRun.get(i));
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
      failedCallables.addAll((List) callablesToRun);
    } finally {
      es.shutdownNow();
    }
  }

  synchronized private void startReadyJobs() {
    Map<Integer, CrunchControlledJob> oldJobs = null;
    oldJobs = this.readyJobs;
    this.readyJobs = new Hashtable<Integer, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      // Limit the number of concurrent running jobs. If we have reached such limit,
      // stop submitting new jobs and wait until some running job completes.
      if (runningJobs.size() < maxRunningJobs) {
        // Submitting Job to Hadoop
        nextJob.setJobSequence(jobSequence);
        jobSequence++;
        nextJob.submit();
      }
      this.addToQueue(nextJob);
    }
  }

  synchronized public void killAllRunningJobs() {
    for (CrunchControlledJob job : runningJobs.values()) {
      if (!job.isCompleted()) {
        try {
          job.killJob();
        } catch (Exception e) {
          log.error("Exception killing job: " + job.getJobName(), e);
        }
      }
    }
  }

  synchronized public boolean allFinished() {
    return (this.waitingJobs.size() == 0 && this.readyJobs.size() == 0
        && this.runningJobs.size() == 0);
  }

  synchronized public boolean anyFailures() {
    return this.failedJobs.size() > 0 || failedCallables.size() > 0;
  }

  public List<PipelineCallable<?>> getFailedCallables() {
    return failedCallables;
  }

  /**
   * Checks the states of the running jobs Update the states of waiting jobs, and submits the jobs in
   * ready state (i.e. whose dependencies are all finished in success).
   */
  public void pollJobStatusAndStartNewOnes() throws IOException, InterruptedException {
    checkRunningJobs();
    checkWaitingJobs();
    executeReadySeqDoFns();
    startReadyJobs();
  }

}
