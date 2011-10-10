/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.crunch;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Base class for all data processing functions in Crunch.
 * 
 * <p>Note that all {@code DoFn} instances implement {@link Serializable},
 * and thus all of their non-transient member variables must implement
 * {@code Serializable} as well. If your DoFn depends on non-serializable
 * classes for data processing, they may be declared as {@code transient}
 * and initialized in the DoFn's {@code initialize} method.
 *
 */
public abstract class DoFn<S, T> implements Serializable {

  private TaskInputOutputContext<?, ?, ?, ?> context;

  /**
   * Called during the job planning phase. Subclasses may override
   * this method in order to modify the configuration of the Job
   * that this DoFn instance belongs to.
   * 
   * @param conf The Configuration instance for the Job.
   */
  public void configure(Configuration conf) {  
  }
  
  /**
   * Processes the records from a {@link PCollection}.
   * 
   * @param input The input record
   * @param emitter The emitter to send the output to
   */
  public abstract void process(S input, Emitter<T> emitter);

  /**
   * Called during the setup of the MapReduce job this {@code DoFn}
   * is associated with. Subclasses may override this method to
   * do appropriate initialization.
   */
  public void initialize() {
  }

  /**
   * Called during the cleanup of the MapReduce job this {@code DoFn}
   * is associated with. Subclasses may override this method to do
   * appropriate cleanup.
   */
  public void cleanup() {
  }

  /**
   * Called during setup to pass the {@link TaskInputOutputContext} to
   * this {@code DoFn} instance.
   */
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    this.context = context;
    initialize();
  }

  /**
   * Returns an estimate of how applying this function to a {@link PCollection}
   * will cause it to change in side. The optimizer uses these estimates to
   * decide where to break up dependent MR jobs into separate Map and Reduce
   * phases in order to minimize I/O.
   * 
   * <p>
   * Subclasses of {@code DoFn} that will substantially alter the size of the
   * resulting {@code PCollection} should override this method.
   */
  public float scaleFactor() {
    return 1.2f;
  }
  
  protected Configuration getConfiguration() {
    return context.getConfiguration();
  }
  
  protected Counter getCounter(Enum<?> counterName) {
    return context.getCounter(counterName);
  }
  
  protected Counter getCounter(String groupName, String counterName) {
    return context.getCounter(groupName, counterName);
  }
  
  protected void progress() {
    context.progress();
  }
  
  protected TaskAttemptID getTaskAttemptID() {
    return context.getTaskAttemptID();
  }
  
  protected void setStatus(String status) {
    context.setStatus(status);
  }
  
  protected String getStatus() {
    return context.getStatus();
  }
}
