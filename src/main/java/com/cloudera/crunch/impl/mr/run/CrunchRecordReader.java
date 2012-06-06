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

package com.cloudera.crunch.impl.mr.run;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

class CrunchRecordReader<K, V> extends RecordReader<K, V> {

  private final RecordReader<K, V> delegate;

  public CrunchRecordReader(InputSplit inputSplit, final TaskAttemptContext context)
      throws IOException, InterruptedException {
    CrunchInputSplit crunchSplit = (CrunchInputSplit) inputSplit;
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
        .newInstance(crunchSplit.getInputFormatClass(), crunchSplit.getConf());
    this.delegate = inputFormat.createRecordReader(
        crunchSplit.getInputSplit(), TaskAttemptContextFactory.create(
            crunchSplit.getConf(), context.getTaskAttemptID()));
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    CrunchInputSplit crunchSplit = (CrunchInputSplit) inputSplit;
    InputSplit delegateSplit = crunchSplit.getInputSplit();
    delegate.initialize(delegateSplit, TaskAttemptContextFactory.create(
        crunchSplit.getConf(), context.getTaskAttemptID()));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

}
