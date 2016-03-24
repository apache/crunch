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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A specialization of {@code Callable} that executes some sequential logic on the client machine as
 * part of an overall Crunch pipeline in order to generate zero or more outputs, some of
 * which may be {@code PCollection} instances that are processed by other jobs in the
 * pipeline.
 *
 * <p>{@code PipelineCallable} is intended to be used to inject auxiliary logic into the control
 * flow of a Crunch pipeline. This can be used for a number of purposes, such as importing or
 * exporting data into a cluster using Apache Sqoop, executing a legacy MapReduce job
 * or Pig/Hive script within a Crunch pipeline, or sending emails or status notifications
 * about the status of a long-running pipeline during its execution.</p>
 *
 * <p>The Crunch planner needs to know three things about a {@code PipelineCallable} instance in order
 * to manage it:
 * <ol>
 *   <li>The {@code Target} and {@code PCollection} instances that must have been materialized
 *   before this instance is allowed to run. This information should be specified via the {@code dependsOn}
 *   methods of the class.</li>
 *   <li>What Outputs will be created after this instance is executed, if any. These outputs may be
 *   new {@code PCollection} instances that are used as inputs in other Crunch jobs. These outputs should
 *   be specified by the {@code getOutput(Pipeline)} method of the class, which will be executed immediately
 *   after this instance is registered with the {@link Pipeline#sequentialDo} method.</li>
 *   <li>The actual logic to execute when the dependent Targets and PCollections have been created in
 *   order to materialize the output data. This is defined in the {@code call} method of the class.</li>
 * </ol>
 * </p>
 *
 * <p>If a given PipelineCallable does not have any dependencies, it will be executed before any jobs are run
 * by the planner. After that, the planner will keep track of when the dependencies of a given instance
 * have been materialized, and then execute the instance as soon as they all exist. The Crunch planner
 * uses a thread pool executor to run multiple {@code PipelineCallable} instances simultaneously, but you can
 * indicate that an instance should be run by itself by overriding the {@code boolean runSingleThreaded()} method
 * below to return true.</p>
 *
 * <p>The {@code call} method returns a {@code Status} to indicate whether it succeeded or failed. A failed
 * instance, or any exceptions/errors thrown by the call method, will cause the overall Crunch pipeline containing
 * this instance to fail.</p>
 *
 * <p>A number of helper methods for accessing the dependent Target/PCollection instances that this instance
 * needs to exist, as well as the {@code Configuration} instance for the overall Pipeline execution, are available
 * as protected methods in this class so that they may be accessed from implementations of {@code PipelineCallable}
 * within the {@code call} method.
 * </p>
 * @param <Output> the output value returned by this instance (Void, PCollection, Pair&lt;PCollection, PCollection&gt;,
 *                 etc.
 */
public abstract class PipelineCallable<Output> implements Callable<PipelineCallable.Status> {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineCallable.class);

  public enum Status { SUCCESS, FAILURE };

  private String name;
  private String message;

  private Map<String, Target> namedTargets = Maps.newHashMap();
  private Map<String, PCollection<?>> namedPCollections = Maps.newHashMap();
  private Configuration conf;

  private boolean outputsGenerated = false;

  /**
   * Clients should override this method to define the outputs that will exist after this instance is
   * executed. These may be PCollections, PObjects, or nothing (which can be indicated with Java's {@code Void}
   * type and a null value.
   *
   * @param pipeline The pipeline that is managing the execution of this instance
   */
  protected abstract Output getOutput(Pipeline pipeline);

  /**
   * Override this method to indicate to the planner that this instance should not be run at the
   * same time as any other {@code PipelineCallable} instances.
   *
   * @return true if this instance should run by itself, false otherwise
   */
  public boolean runSingleThreaded() {
    return false;
  }

  /**
   * Requires that the given {@code Target} exists before this instance may be
   * executed.
   *
   * @param label A string that can be used to retrieve the given Target inside of the {@code call} method.
   * @param t the {@code Target} itself
   * @return this instance
   */
  public PipelineCallable<Output> dependsOn(String label, Target t) {
    Preconditions.checkNotNull(label, "label");
    if (outputsGenerated) {
      throw new IllegalStateException(
          "Dependencies may not be added to a PipelineCallable after its outputs have been generated");
    }
    if (namedTargets.containsKey(label)) {
      throw new IllegalStateException("Label " + label + " cannot be reused for multiple targets");
    }
    this.namedTargets.put(label, t);
    return this;
  }

  /**
   * Requires that the given {@code PCollection} be materialized to disk before this instance may be
   * executed.
   *
   * @param label A string that can be used to retrieve the given PCollection inside of the {@code call} method.
   * @param pcollect the {@code PCollection} itself
   * @return this instance
   */
  public PipelineCallable<Output> dependsOn(String label, PCollection<?> pcollect) {
    Preconditions.checkNotNull(label, "label");
    if (outputsGenerated) {
      throw new IllegalStateException(
          "Dependencies may not be added to a PipelineCallable after its outputs have been generated");
    }
    if (namedPCollections.containsKey(label)) {
      throw new IllegalStateException("Label " + label + " cannot be reused for multiple PCollections");
    }
    this.namedPCollections.put(label, pcollect);
    return this;
  }

  /**
   * Called by the {@code Pipeline} when this instance is registered with {@code Pipeline#sequentialDo}. In general,
   * clients should override the protected {@code getOutput(Pipeline)} method instead of this one.
   */
  public Output generateOutput(Pipeline pipeline) {
    if (outputsGenerated == true) {
      throw new IllegalStateException("PipelineCallable.generateOutput should only be called once");
    }
    outputsGenerated = true;
    this.conf = pipeline.getConfiguration();
    return getOutput(pipeline);
  }

  /**
   * Returns the name of this instance.
   */
  public String getName() {
    return name == null ? this.getClass().getName() : name;
  }

  /**
   * Use the given name to identify this instance in the logs.
   */
  public PipelineCallable<Output> named(String name) {
    this.name = name;
    return this;
  }

  /**
   * Returns a message associated with this callable's execution, especially in case of errors.
   */
  public String getMessage() {
    if (message == null) {
      LOG.warn("No message specified for PipelineCallable instance \"{}\". Consider overriding PipelineCallable.getMessage()", getName());
      return toString();
    }
    return message;
  }

  /**
   * Sets a message associated with this callable's execution, especially in case of errors.
   */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * The {@code Configuration} instance for the {@code Pipeline} this callable is registered with.
   */
  protected Configuration getConfiguration() {
    return conf;
  }

  /**
   * Returns the {@code Target} associated with the given label in the dependencies list,
   * or null if no such target exists.
   */
  protected Target getTarget(String label) {
    return namedTargets.get(label);
  }

  /**
   * Returns the {@code PCollection} associated with the given label in the dependencies list,
   * or null if no such instance exists.
   */
  protected PCollection getPCollection(String label) {
    return namedPCollections.get(label);
  }

  /**
   * Returns the only PCollection this instance depends on. Only valid in the case that this callable
   * has precisely one dependency.
   */
  protected PCollection getOnlyPCollection() {
    return Iterables.getOnlyElement(namedPCollections.values());
  }

  /**
   * Returns the mapping of labels to PCollection dependencies for this instance.
   */
  public Map<String, PCollection<?>> getAllPCollections() {
    return ImmutableMap.copyOf(namedPCollections);
  }

  /**
   * Returns the mapping of labels to Target dependencies for this instance.
   */
  public Map<String, Target> getAllTargets() {
    return ImmutableMap.copyOf(namedTargets);
  }
}
