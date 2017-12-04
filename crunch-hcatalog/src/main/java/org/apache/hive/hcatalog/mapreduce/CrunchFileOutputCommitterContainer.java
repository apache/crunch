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

import org.apache.hadoop.mapred.OutputCommitter;;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Thin extension to construct valid
 * {@link org.apache.hadoop.mapred.TaskAttemptID}, and remove the crunch named
 * output from the {@link org.apache.hadoop.mapreduce.TaskAttemptID}.
 */
public class CrunchFileOutputCommitterContainer extends FileOutputCommitterContainer {

  private final boolean dynamicPartitioningUsed;

  /**
   * @param context
   *          current JobContext
   * @param baseCommitter
   *          OutputCommitter to contain
   * @throws IOException
   */
  public CrunchFileOutputCommitterContainer(JobContext context, OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
    dynamicPartitioningUsed = HCatOutputFormat.getJobInfo(context.getConfiguration()).isDynamicPartitioningUsed();
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtils.getOldTaskAttemptContext(context));
    } else {
      // called explicitly through FileRecordWriterContainer.close() if dynamic
      // - return false by default
      return true;
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      // See HCATALOG-499
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().commitTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().commitTask(context);
      } finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().abortTask(HCatMapRedUtils.getOldTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().abortTask(context);
      } finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }
}
