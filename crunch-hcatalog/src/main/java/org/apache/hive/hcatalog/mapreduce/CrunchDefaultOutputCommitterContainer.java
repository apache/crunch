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
package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A thin extension of the Hive {@link DefaultOutputCommitterContainer}. This is
 * to insert crunch specific logic to strip the named output from the
 * TaskAttemptID.
 */
class CrunchDefaultOutputCommitterContainer extends DefaultOutputCommitterContainer {

  /**
   * @param context
   *          current JobContext
   * @param baseCommitter
   *          OutputCommitter to contain
   * @throws IOException
   */
  public CrunchDefaultOutputCommitterContainer(JobContext context, OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().setupTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().abortTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().commitTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtils.getOldTaskAttemptContext(context));
  }
}
