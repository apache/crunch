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

import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrcCrunchOutputFormat extends FileOutputFormat<NullWritable, OrcWritable> {
  
  private OrcNewOutputFormat outputFormat = new OrcNewOutputFormat();

  @Override
  public RecordWriter<NullWritable, OrcWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    RecordWriter<NullWritable, Writable> writer = outputFormat.getRecordWriter(job);
    return new OrcCrunchRecordWriter(writer);
  }
  
  static class OrcCrunchRecordWriter extends RecordWriter<NullWritable, OrcWritable> {
    
    private final RecordWriter<NullWritable, Writable> writer;
    private final OrcSerde orcSerde;
    
    OrcCrunchRecordWriter(RecordWriter<NullWritable, Writable> writer) {
      this.writer = writer;
      this.orcSerde = new OrcSerde();
    }

    @Override
    public void write(NullWritable key, OrcWritable value) throws IOException,
        InterruptedException {
      if (value.get() == null) {
        throw new NullPointerException("Cannot write null records to orc file");
      }
      writer.write(key, orcSerde.serialize(value.get(), value.getObjectInspector()));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      writer.close(context);
    }
    
  }

}
