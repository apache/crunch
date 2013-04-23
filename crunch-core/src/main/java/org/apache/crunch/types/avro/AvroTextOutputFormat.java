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
package org.apache.crunch.types.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvroTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
  class DatumRecordTextWriter extends RecordWriter<K, V> {
    private RecordWriter lineRecordWriter;

    public DatumRecordTextWriter(RecordWriter recordWriter) {
      this.lineRecordWriter = recordWriter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      lineRecordWriter.close(context);
    }

    @Override
    public void write(K arg0, V arg1) throws IOException, InterruptedException {
      lineRecordWriter.write(getData(arg0), getData(arg1));
    }

    private Object getData(Object o) {
      Object data = o;
      if (o instanceof AvroWrapper) {
        data = ((AvroWrapper) o).datum();
      }
      return data;
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    RecordWriter<K, V> recordWriter = super.getRecordWriter(context);
    return new DatumRecordTextWriter(recordWriter);
  }

}
