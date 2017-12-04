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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.List;

/**
 * Common helper methods for translating between v1 and v2 of map reduce
 */
public class HCatMapRedUtils {

  public static org.apache.hadoop.mapred.TaskAttemptContext getOldTaskAttemptContext(TaskAttemptContext context) {
    return new TaskAttemptContextImpl(new JobConf(context.getConfiguration()), getTaskAttemptID(context));
  }

  /**
   * Creates a {@code TaskAttemptID} from the provided TaskAttemptContext. This
   * also performs logic to strip the crunch named output from the TaskAttemptID
   * already associated with the TaskAttemptContext. The TaskAttemptID requires
   * there to be six parts, separated by "_". With the named output the JobID
   * has 7 parts. That needs to be stripped away before a new TaskAttemptID can
   * be constructed.
   *
   * @param context
   *          The TaskAttemptContext
   * @return A TaskAttemptID with the crunch named output removed
   */
  public static TaskAttemptID getTaskAttemptID(TaskAttemptContext context) {
    String taskAttemptId = context.getTaskAttemptID().toString();
    List<String> taskAttemptIDParts = Lists.newArrayList(taskAttemptId.split("_"));
    if (taskAttemptIDParts.size() < 7)
      return TaskAttemptID.forName(taskAttemptId);

    // index 2 is the 3rd element in the task attempt id, which will be the
    // named output
    taskAttemptIDParts.remove(2);
    String reducedTaskAttemptId = StringUtils.join(taskAttemptIDParts, "_");
    return TaskAttemptID.forName(reducedTaskAttemptId);
  }
}
