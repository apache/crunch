/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.impl.mr.run;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 *
 */
@SuppressWarnings("unchecked")
public class TaskAttemptContextFactory {

  private static final Log LOG = LogFactory.getLog(TaskAttemptContextFactory.class);

  private static final TaskAttemptContextFactory INSTANCE = new TaskAttemptContextFactory();
  
  public static TaskAttemptContext create(Configuration conf, TaskAttemptID taskAttemptId) {
    return INSTANCE.createInternal(conf, taskAttemptId);
  }
  
  private Constructor taskAttemptConstructor;
  
  private TaskAttemptContextFactory() {
    Class implClass = TaskAttemptContext.class;
    if (implClass.isInterface()) {
      try {
        implClass = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      } catch (ClassNotFoundException e) {
        LOG.fatal("Could not find TaskAttemptContextImpl class, exiting", e);
      }
    }
    try {
      this.taskAttemptConstructor = implClass.getConstructor(Configuration.class, TaskAttemptID.class);
    } catch (Exception e) {
      LOG.fatal("Could not access TaskAttemptContext constructor, exiting", e);
    }
  }
  
  private TaskAttemptContext createInternal(Configuration conf, TaskAttemptID taskAttemptId) {
    try {
      return (TaskAttemptContext) taskAttemptConstructor.newInstance(conf, taskAttemptId);
    } catch (Exception e) {
      LOG.error("Could not construct a TaskAttemptContext instance", e);
      return null;
    }
  }
}
