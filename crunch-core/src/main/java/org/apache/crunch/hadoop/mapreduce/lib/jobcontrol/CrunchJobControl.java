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

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.impl.mr.MRJob.State;

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

  private Log log = LogFactory.getLog(CrunchJobControl.class);

  private final String groupName;

  /**
   * Construct a job control for a group of jobs.
   * 
   * @param groupName
   *          a name identifying this group
   */
  public CrunchJobControl(String groupName) {
    this.waitingJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.readyJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.runningJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.successfulJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.failedJobs = new Hashtable<Integer, CrunchControlledJob>();
    this.groupName = groupName;
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

  synchronized private void startReadyJobs() {
    Map<Integer, CrunchControlledJob> oldJobs = null;
    oldJobs = this.readyJobs;
    this.readyJobs = new Hashtable<Integer, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      // Submitting Job to Hadoop
      nextJob.submit();
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
    return this.waitingJobs.size() == 0 && this.readyJobs.size() == 0
        && this.runningJobs.size() == 0;
  }

  /**
   * Checks the states of the running jobs Update the states of waiting jobs, and submits the jobs in
   * ready state (i.e. whose dependencies are all finished in success).
   */
  public void pollJobStatusAndStartNewOnes() throws IOException, InterruptedException {
    checkRunningJobs();
    checkWaitingJobs();
    startReadyJobs();
  }
}
