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
package org.apache.crunch.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OrcCrunchInputFormat extends InputFormat<NullWritable, OrcWritable> {

  private OrcNewInputFormat inputFormat = new OrcNewInputFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return inputFormat.getSplits(context);
  }

  @Override
  public RecordReader<NullWritable, OrcWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    RecordReader<NullWritable, OrcStruct> reader = inputFormat.createRecordReader(
        split, context);
    return new OrcCrunchRecordReader(reader);
  }

  static class OrcCrunchRecordReader extends RecordReader<NullWritable, OrcWritable> {
    
    private final RecordReader<NullWritable, OrcStruct> reader;
    private OrcWritable value = new OrcWritable();

    OrcCrunchRecordReader(RecordReader<NullWritable, OrcStruct> reader) {
      this.reader = reader;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException,
        InterruptedException {
      return NullWritable.get();
    }

    @Override
    public OrcWritable getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean hasNext = reader.nextKeyValue();
      if (hasNext) {
        value.set(reader.getCurrentValue());
      }
      return hasNext;
    }
    
  }

}
