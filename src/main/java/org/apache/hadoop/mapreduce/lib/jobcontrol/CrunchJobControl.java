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

package org.apache.hadoop.mapreduce.lib.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob.State;

/**
 * This class encapsulates a set of MapReduce jobs and its dependency.
 * 
 * It tracks the states of the jobs by placing them into different tables
 * according to their states.
 * 
 * This class provides APIs for the client app to add a job to the group and to
 * get the jobs in the group in different states. When a job is added, an ID
 * unique to the group is assigned to the job.
 * 
 * This class has a thread that submits jobs when they become ready, monitors
 * the states of the running jobs, and updates the states of jobs based on the
 * state changes of their depending jobs states. The class provides APIs for
 * suspending/resuming the thread, and for stopping the thread.
 * 
 * TODO This is mostly a copy of the JobControl class in Hadoop MapReduce core. 
 * Once the location and interface of the class are more stable in CDH, this class 
 * should be removed completely and be based on the hadoop-core class.
 */
public class CrunchJobControl implements Runnable {

  // The thread can be in one of the following state
  public static enum ThreadState {
    RUNNING, SUSPENDED, STOPPED, STOPPING, READY
  };

  private ThreadState runnerState; // the thread state

  private Map<String, CrunchControlledJob> waitingJobs;
  private Map<String, CrunchControlledJob> readyJobs;
  private Map<String, CrunchControlledJob> runningJobs;
  private Map<String, CrunchControlledJob> successfulJobs;
  private Map<String, CrunchControlledJob> failedJobs;

  private long nextJobID;
  private String groupName;

  /**
   * Construct a job control for a group of jobs.
   * 
   * @param groupName
   *          a name identifying this group
   */
  public CrunchJobControl(String groupName) {
    this.waitingJobs = new Hashtable<String, CrunchControlledJob>();
    this.readyJobs = new Hashtable<String, CrunchControlledJob>();
    this.runningJobs = new Hashtable<String, CrunchControlledJob>();
    this.successfulJobs = new Hashtable<String, CrunchControlledJob>();
    this.failedJobs = new Hashtable<String, CrunchControlledJob>();
    this.nextJobID = -1;
    this.groupName = groupName;
    this.runnerState = ThreadState.READY;
  }

  private static List<CrunchControlledJob> toList(Map<String, CrunchControlledJob> jobs) {
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

  private String getNextJobID() {
    nextJobID += 1;
    return this.groupName + this.nextJobID;
  }

  private static void addToQueue(CrunchControlledJob aJob,
      Map<String, CrunchControlledJob> queue) {
    synchronized (queue) {
      queue.put(aJob.getJobID(), aJob);
    }
  }

  private void addToQueue(CrunchControlledJob aJob) {
    Map<String, CrunchControlledJob> queue = getQueue(aJob.getJobState());
    addToQueue(aJob, queue);
  }

  private Map<String, CrunchControlledJob> getQueue(State state) {
    Map<String, CrunchControlledJob> retv = null;
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
  synchronized public String addJob(CrunchControlledJob aJob) {
    String id = this.getNextJobID();
    aJob.setJobID(id);
    aJob.setJobState(State.WAITING);
    this.addToQueue(aJob);
    return id;
  }

  /**
   * Add a collection of jobs
   * 
   * @param jobs
   */
  public void addJobCollection(Collection<CrunchControlledJob> jobs) {
    for (CrunchControlledJob job : jobs) {
      addJob(job);
    }
  }

  /**
   * @return the thread state
   */
  public ThreadState getThreadState() {
    return this.runnerState;
  }

  /**
   * set the thread state to STOPPING so that the thread will stop when it wakes
   * up.
   */
  public void stop() {
    this.runnerState = ThreadState.STOPPING;
  }

  /**
   * suspend the running thread
   */
  public void suspend() {
    if (this.runnerState == ThreadState.RUNNING) {
      this.runnerState = ThreadState.SUSPENDED;
    }
  }

  /**
   * resume the suspended thread
   */
  public void resume() {
    if (this.runnerState == ThreadState.SUSPENDED) {
      this.runnerState = ThreadState.RUNNING;
    }
  }

  synchronized private void checkRunningJobs() throws IOException,
      InterruptedException {

    Map<String, CrunchControlledJob> oldJobs = null;
    oldJobs = this.runningJobs;
    this.runningJobs = new Hashtable<String, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  synchronized private void checkWaitingJobs() throws IOException,
      InterruptedException {
    Map<String, CrunchControlledJob> oldJobs = null;
    oldJobs = this.waitingJobs;
    this.waitingJobs = new Hashtable<String, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      nextJob.checkState();
      this.addToQueue(nextJob);
    }
  }

  synchronized private void startReadyJobs() {
    Map<String, CrunchControlledJob> oldJobs = null;
    oldJobs = this.readyJobs;
    this.readyJobs = new Hashtable<String, CrunchControlledJob>();

    for (CrunchControlledJob nextJob : oldJobs.values()) {
      // Submitting Job to Hadoop
      nextJob.submit();
      this.addToQueue(nextJob);
    }
  }

  synchronized public boolean allFinished() {
    return this.waitingJobs.size() == 0 && this.readyJobs.size() == 0
        && this.runningJobs.size() == 0;
  }

  /**
   * The main loop for the thread. The loop does the following: Check the states
   * of the running jobs Update the states of waiting jobs Submit the jobs in
   * ready state
   */
  public void run() {
    this.runnerState = ThreadState.RUNNING;
    while (true) {
      while (this.runnerState == ThreadState.SUSPENDED) {
        try {
          Thread.sleep(5000);
        } catch (Exception e) {

        }
      }
      try {
        checkRunningJobs();
        checkWaitingJobs();
        startReadyJobs();
      } catch (Exception e) {
        this.runnerState = ThreadState.STOPPED;
      }
      if (this.runnerState != ThreadState.RUNNING
          && this.runnerState != ThreadState.SUSPENDED) {
        break;
      }
      try {
        Thread.sleep(5000);
      } catch (Exception e) {

      }
      if (this.runnerState != ThreadState.RUNNING
          && this.runnerState != ThreadState.SUSPENDED) {
        break;
      }
    }
    this.runnerState = ThreadState.STOPPED;
  }

}
