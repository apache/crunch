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

import java.io.IOException;

import org.apache.crunch.hadoop.mapreduce.TaskAttemptContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

class CrunchRecordReader<K, V> extends RecordReader<K, V> {

  private RecordReader<K, V> curReader;
  private CrunchInputSplit crunchSplit;
  private CombineFileSplit combineFileSplit;
  private TaskAttemptContext context;
  private int idx;
  private long progress;

  public CrunchRecordReader(InputSplit inputSplit, final TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.crunchSplit = (CrunchInputSplit) inputSplit;
    if (crunchSplit.get() instanceof CombineFileSplit) {
      combineFileSplit = (CombineFileSplit) crunchSplit.get();
    }
    this.context = context;
    Configuration conf = crunchSplit.getConf();
    if (conf == null) {
      conf = context.getConfiguration();
      crunchSplit.setConf(conf);
    }
    initNextRecordReader();
  }

  private boolean initNextRecordReader() throws IOException, InterruptedException {
    if (combineFileSplit != null) {
      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += combineFileSplit.getLength(idx - 1);
        }
      }
      // if all chunks have been processed, nothing more to do.
      if (idx == combineFileSplit.getNumPaths()) {
        return false;
      }
    } else if (idx > 0) {
      return false;
    }

    idx++;
    Configuration conf = crunchSplit.getConf();
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils.newInstance(
        crunchSplit.getInputFormatClass(),
        conf);
    this.curReader = inputFormat.createRecordReader(getDelegateSplit(),
        TaskAttemptContextFactory.create(conf, context.getTaskAttemptID()));
    return true;
  }

  private InputSplit getDelegateSplit() throws IOException {
    if (combineFileSplit != null) {
      return new FileSplit(combineFileSplit.getPath(idx - 1),
          combineFileSplit.getOffset(idx - 1),
          combineFileSplit.getLength(idx - 1),
          combineFileSplit.getLocations());
    } else {
      return crunchSplit.get();
    }
  }

  @Override
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return curReader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return curReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    float curProgress = 0;    // bytes processed in current split
    if (null != curReader) {
      curProgress = (float)(curReader.getProgress() * getCurLength());
    }
    return Math.min(1.0f,  (progress + curProgress)/getOverallLength());
  }

  private long getCurLength() {
    if (combineFileSplit == null) {
      return 1L;
    } else {
      return combineFileSplit.getLength(idx - 1);
    }
  }

  private float getOverallLength() {
    if (combineFileSplit == null) {
      return 1.0f;
    } else {
      return (float) combineFileSplit.getLength();
    }
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    this.crunchSplit = (CrunchInputSplit) inputSplit;
    this.context = context;
    Configuration conf = crunchSplit.getConf();
    if (conf == null) {
      conf = context.getConfiguration();
      crunchSplit.setConf(conf);
    }
    if (crunchSplit.get() instanceof CombineFileSplit) {
      combineFileSplit = (CombineFileSplit) crunchSplit.get();
    }
    if (curReader != null) {
      curReader.initialize(getDelegateSplit(),
          TaskAttemptContextFactory.create(conf, context.getTaskAttemptID()));
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while ((curReader == null) || !curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        return false;
      }
      if (curReader != null) {
        curReader.initialize(getDelegateSplit(),
            TaskAttemptContextFactory.create(crunchSplit.getConf(), context.getTaskAttemptID()));
      }
    }
    return true;
  }
}
