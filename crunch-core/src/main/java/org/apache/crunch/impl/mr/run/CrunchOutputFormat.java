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

import org.apache.crunch.io.CrunchOutputs;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CrunchOutputFormat<K, V> extends OutputFormat<K, V> {
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new RecordWriter<K, V>() {
      @Override
      public void write(K k, V v) throws IOException, InterruptedException {
      }

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      }
    };
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    CrunchOutputs.checkOutputSpecs(jobContext);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return CrunchOutputs.getOutputCommitter(taskAttemptContext);
  }
}
