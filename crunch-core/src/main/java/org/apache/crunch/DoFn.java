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
package org.apache.crunch;

import java.io.Serializable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.base.Preconditions;

/**
 * Base class for all data processing functions in Crunch.
 * 
 * <p>
 * Note that all {@code DoFn} instances implement {@link Serializable}, and thus
 * all of their non-transient member variables must implement
 * {@code Serializable} as well. If your DoFn depends on non-serializable
 * classes for data processing, they may be declared as {@code transient} and
 * initialized in the DoFn's {@code initialize} method.
 * 
 */
public abstract class DoFn<S, T> implements Serializable {
  /** This will be null prior to being set in {@link #setContext(TaskInputOutputContext)}. */
  @CheckForNull
  private transient TaskInputOutputContext<?, ?, ?, ?> context;
  /** This will be null prior to being set in {@link #setConfiguration(Configuration)}. */
  @CheckForNull
  private transient Configuration conf;

  /**
   * Configure this DoFn. Subclasses may override this method to modify the
   * configuration of the Job that this DoFn instance belongs to.
   * 
   * <p>
   * Called during the job planning phase by the crunch-client.
   * </p>
   * 
   * @param conf
   *          The Configuration instance for the Job.
   */
  public void configure(Configuration conf) {
  }

  /**
   * Initialize this DoFn. This initialization will happen before the actual
   * {@link #process(Object, Emitter)} is triggered. Subclasses may override
   * this method to do appropriate initialization.
   * 
   * <p>
   * Called during the setup of the job instance this {@code DoFn} is associated
   * with.
   * </p>
   * 
   */
  public void initialize() {
  }

  /**
   * Processes the records from a {@link PCollection}.
   * 
   * <br/>
   * <br/>
   * <b>Note:</b> Crunch can reuse a single input record object whose content
   * changes on each {@link #process(Object, Emitter)} method call. This
   * functionality is imposed by Hadoop's <a href=
   * "http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/Reducer.html"
   * >Reducer</a> implementation: <i>The framework will reuse the key and value
   * objects that are passed into the reduce, therefore the application should
   * clone the objects they want to keep a copy of.</i>
   * 
   * @param input
   *          The input record.
   * @param emitter
   *          The emitter to send the output to
   */
  public abstract void process(S input, Emitter<T> emitter);

  /**
   * Called during the cleanup of the MapReduce job this {@code DoFn} is
   * associated with. Subclasses may override this method to do appropriate
   * cleanup.
   * 
   * @param emitter
   *          The emitter that was used for output
   */
  public void cleanup(Emitter<T> emitter) {
  }

  /**
   * Called during setup to pass the {@link TaskInputOutputContext} to this
   * {@code DoFn} instance. The specified {@code TaskInputOutputContext} must not be null.
   */
  public void setContext(@Nonnull TaskInputOutputContext<?, ?, ?, ?> context) {
    Preconditions.checkNotNull(context);
    this.context = context;
  }

  /**
   * Called during the setup of an initialized {@link org.apache.crunch.types.PType} that
   * relies on this instance.
   *
   * @param conf
   *          The non-null configuration for the {@code PType} being initialized
   */
  public void setConfiguration(@Nonnull Configuration conf) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
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
    return 0.99f;
  }

  /**
   * By default, Crunch will do a defensive deep copy of the outputs of a
   * DoFn when there are multiple downstream consumers of that item, in order to
   * prevent the downstream functions from making concurrent modifications to
   * data objects. This introduces some extra overhead in cases where you know
   * that the downstream code is only reading the objects and not modifying it,
   * so you can disable this feature by overriding this function to
   * return {@code true}.
   */
  public boolean disableDeepCopy() {
    return false;
  }

  protected TaskInputOutputContext<?, ?, ?, ?> getContext() {
    return context;
  }

  protected Configuration getConfiguration() {
    if (conf != null) {
      return conf;
    } else if (context != null) {
      return context.getConfiguration();
    } else {
      return null;
    }
  }

  /**
   * @deprecated The {@link Counter} class changed incompatibly between Hadoop 1 and 2
   * (from a class to an interface) so user programs should avoid this method and use
   * one of the <code>increment</code> methods instead, such as {@link #increment(Enum)}.
   */
  @Deprecated
  protected Counter getCounter(Enum<?> counterName) {
    if (context == null) {
      return null;
    }
    return context.getCounter(counterName);
  }

  /**
   * @deprecated The {@link Counter} class changed incompatibly between Hadoop 1 and 2
   * (from a class to an interface) so user programs should avoid this method and use
   * one of the <code>increment</code> methods instead, such as {@link #increment(Enum)}.
   */
  @Deprecated
  protected Counter getCounter(String groupName, String counterName) {
    if (context == null) {
        return null;
    }
    return context.getCounter(groupName, counterName);
  }

  protected void increment(String groupName, String counterName) {
    increment(groupName, counterName, 1);
  }

  protected void increment(String groupName, String counterName, long value) {
    if (context != null) {
      context.getCounter(groupName, counterName).increment(value);
    }
  }

  protected void increment(Enum<?> counterName) {
    increment(counterName, 1);
  }

  protected void increment(Enum<?> counterName, long value) {
    if (context != null) {
      context.getCounter(counterName).increment(value);
    }
  }

  protected void progress() {
    if (context != null) {
      context.progress();
    }
  }

  protected TaskAttemptID getTaskAttemptID() {
    if (context == null) {
      return null;
    }
    return context.getTaskAttemptID();
  }

  protected void setStatus(String status) {
    if (context != null) {
      context.setStatus(status);
    }
  }

  protected String getStatus() {
    if (context == null) {
      return null;
    }
    return context.getStatus();
  }

}
