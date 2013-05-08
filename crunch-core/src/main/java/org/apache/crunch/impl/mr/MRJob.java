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
package org.apache.crunch.impl.mr;

import org.apache.hadoop.mapreduce.Job;

import java.util.List;

/**
 * A Hadoop MapReduce job managed by Crunch.
 */
public interface MRJob {

  /** A job will be in one of the following states. */
  public static enum State {
    SUCCESS, WAITING, RUNNING, READY, FAILED, DEPENDENT_FAILED
  };

  /** @return the Job ID assigned by Crunch */
  int getJobID();

  /** @return the internal Hadoop MapReduce job */
  Job getJob();

  /** @return the depending jobs of this job */
  List<MRJob> getDependentJobs();

  /** @return the state of this job */
  State getJobState();
}
