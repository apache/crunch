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

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A handle to allow clients to control a Crunch pipeline as it runs.
 *
 * This interface is thread-safe.
 */
public interface PipelineExecution extends ListenableFuture<PipelineResult> {

  enum Status { READY, RUNNING, SUCCEEDED, FAILED, KILLED }

  /** Returns the .dot file that allows a client to graph the Crunch execution plan for this
   * pipeline.
   */
  String getPlanDotFile();

  /**
   * Returns all .dot files that allows a client to graph the Crunch execution plan internals.
   * Key is the name of the dot file and the value is the file itself
   */
  Map<String, String> getNamedDotFiles();

  /** Blocks until pipeline completes or the specified waiting time elapsed. */
   void waitFor(long timeout, TimeUnit timeUnit) throws InterruptedException;

   /** Blocks until pipeline completes, i.e. {@code SUCCEEDED}, {@code FAILED} or {@code KILLED}. */
  void waitUntilDone() throws InterruptedException;

  Status getStatus();

  /** Retrieve the result of a pipeline if it has been completed, otherwise {@code null}. */
  PipelineResult getResult();

  /**
   * Kills the pipeline if it is running, no-op otherwise.
   *
   * This method only delivers a kill signal to the pipeline, and does not guarantee the pipeline exits on return.
   * To wait for completely exits, use {@link #waitUntilDone()} after this call.
   */
  void kill() throws InterruptedException;
}
