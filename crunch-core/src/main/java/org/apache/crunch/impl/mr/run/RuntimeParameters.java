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
package org.apache.crunch.impl.mr.run;

/**
 * Parameters used during the runtime execution.
 */
public final class RuntimeParameters {

  public static final String DEBUG = "crunch.debug";

  public static final String TMP_DIR = "crunch.tmp.dir";

  public static final String LOG_JOB_PROGRESS = "crunch.log.job.progress";

  /**
   * Runtime property which indicates that a {@link org.apache.crunch.Source} should attempt to combine small files
   * to reduce overhead by default splits.  Unless overridden by the {@code Source} implementation it will default to
   * {@code true}.
   */
  public static final String DISABLE_COMBINE_FILE = "crunch.disable.combine.file";

  public static final String COMBINE_FILE_BLOCK_SIZE = "crunch.combine.file.block.size";

  public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";

  public static final String DISABLE_DEEP_COPY = "crunch.disable.deep.copy";

  public static final String MAX_RUNNING_JOBS = "crunch.max.running.jobs";

  public static final String FILE_TARGET_MAX_THREADS = "crunch.file.target.max.threads";

  public static final String MAX_POLL_INTERVAL = "crunch.max.poll.interval";

  public static final String FILE_TARGET_USE_DISTCP = "crunch.file.target.use.distcp";

  public static final String FILE_TARGET_MAX_DISTCP_TASKS = "crunch.file.target.max.distcp.tasks";

  public static final String FILE_TARGET_MAX_DISTCP_TASK_BANDWIDTH_MB = "crunch.file.target.max.distcp.task.bandwidth.mb";

  // Not instantiated
  private RuntimeParameters() {
  }
}
