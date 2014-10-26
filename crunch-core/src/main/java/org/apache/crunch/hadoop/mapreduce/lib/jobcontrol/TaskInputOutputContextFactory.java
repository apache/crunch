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
package org.apache.crunch.hadoop.mapreduce.lib.jobcontrol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class TaskInputOutputContextFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TaskInputOutputContextFactory.class);

  private static final TaskInputOutputContextFactory INSTANCE = new TaskInputOutputContextFactory();

  public static TaskInputOutputContext create(
      Configuration conf,
      TaskAttemptID taskAttemptId,
      StatusReporter reporter) {
    return INSTANCE.createInternal(conf, taskAttemptId, reporter);
  }

  private Constructor<? extends TaskInputOutputContext> taskIOConstructor;
  private int arity;

  private TaskInputOutputContextFactory() {
    String ic = TaskInputOutputContext.class.isInterface() ?
        "org.apache.hadoop.mapreduce.task.MapContextImpl" :
        "org.apache.hadoop.mapreduce.MapContext";
    try {
      Class<? extends TaskInputOutputContext> implClass = (Class<? extends TaskInputOutputContext>) Class.forName(ic);
      this.taskIOConstructor = (Constructor<? extends TaskInputOutputContext>) implClass.getConstructor(
          Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class,
          OutputCommitter.class, StatusReporter.class, InputSplit.class);
      this.arity = 7;
    } catch (Exception e) {
      LOG.error("Could not access TaskInputOutputContext constructor, exiting", e);
    }
  }

  private TaskInputOutputContext createInternal(Configuration conf, TaskAttemptID taskAttemptId,
                                                StatusReporter reporter) {
    Object[] args = new Object[arity];
    args[0] = conf;
    args[1] = taskAttemptId;
    args[5] = reporter;
    try {
      return taskIOConstructor.newInstance(args);
    } catch (Exception e) {
      LOG.error("Could not construct a TaskInputOutputContext instance", e);
      throw new RuntimeException(e);
    }
  }
}
