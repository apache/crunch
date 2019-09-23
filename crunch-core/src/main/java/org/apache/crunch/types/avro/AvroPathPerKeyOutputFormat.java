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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A {@link FileOutputFormat} that takes in a {@link Utf8} and an Avro record and writes the Avro records to
 * a sub-directory of the output path whose name is equal to the string-form of the {@code Utf8}.
 *
 * This {@code OutputFormat} only keeps one {@code RecordWriter} open at a time, so it's a very good idea to write
 * out all of the records for the same key at the same time within each partition so as not to be frequently opening
 * and closing files.
 */
public class AvroPathPerKeyOutputFormat<T> extends FileOutputFormat<AvroWrapper<Pair<Utf8, T>>, NullWritable> {
  @Override
  public RecordWriter<AvroWrapper<Pair<Utf8, T>>, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    Path basePath = new Path(getOutputPath(taskAttemptContext), conf.get("mapreduce.output.basename", "out0"));
    return new AvroFilePerKeyRecordWriter<T>(basePath, getUniqueFile(taskAttemptContext, "part", ".avro"), conf);
  }

  private class AvroFilePerKeyRecordWriter<T> extends RecordWriter<AvroWrapper<Pair<Utf8, T>>, NullWritable> {

    private final Path basePath;
    private final String uniqueFileName;
    private final Configuration conf;
    private String currentKey;
    private DataFileWriter<T> currentWriter;

    public AvroFilePerKeyRecordWriter(Path basePath, String uniqueFileName, Configuration conf) {
      this.basePath = basePath;
      this.uniqueFileName = uniqueFileName;
      this.conf = conf;
    }

    @Override
    public void write(AvroWrapper<Pair<Utf8, T>> record, NullWritable n) throws IOException, InterruptedException {
      String key = record.datum().key().toString();
      if (!key.equals(currentKey)) {
        if (currentWriter != null) {
          currentWriter.close();
        }
        currentKey = key;
        Path dir = new Path(basePath, key);
        FileSystem fs = dir.getFileSystem(conf);
        if (!fs.exists(dir)) {
          fs.mkdirs(dir);
        }
        currentWriter = AvroOutputFormat.getDataFileWriter(new Path(dir, uniqueFileName), conf);
      }
      currentWriter.append(record.datum().value());
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      if (currentWriter != null) {
        currentWriter.close();
        currentKey = null;
        currentWriter = null;
      }
    }
  }
}
