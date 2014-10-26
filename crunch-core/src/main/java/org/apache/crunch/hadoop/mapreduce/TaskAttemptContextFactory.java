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
package org.apache.crunch.hadoop.mapreduce;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class that allows us to hide the fact that {@code TaskAttemptContext} is a class in
 * Hadoop 1.x.x and an interface in Hadoop 2.x.x.
 */
@SuppressWarnings("unchecked")
public class TaskAttemptContextFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptContextFactory.class);

  private static final TaskAttemptContextFactory INSTANCE = new TaskAttemptContextFactory();

  public static TaskAttemptContext create(Configuration conf, TaskAttemptID taskAttemptId) {
    return INSTANCE.createInternal(conf, taskAttemptId);
  }

  private Constructor<TaskAttemptContext> taskAttemptConstructor;

  private TaskAttemptContextFactory() {
    Class<TaskAttemptContext> implClass = TaskAttemptContext.class;
    if (implClass.isInterface()) {
      try {
        implClass = (Class<TaskAttemptContext>) Class.forName(
            "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      } catch (ClassNotFoundException e) {
        LOG.error("Could not find TaskAttemptContextImpl class, exiting", e);
      }
    }
    try {
      this.taskAttemptConstructor = implClass.getConstructor(Configuration.class, TaskAttemptID.class);
    } catch (Exception e) {
      LOG.error("Could not access TaskAttemptContext constructor, exiting", e);
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
