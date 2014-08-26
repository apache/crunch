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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An {@link org.apache.hadoop.mapreduce.OutputFormat} for Avro data files. */
public class AvroOutputFormat<T> extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  public static <S>  DataFileWriter<S> getDataFileWriter(Path path, Configuration conf) throws IOException {
    Schema schema = null;
    String outputName = conf.get("crunch.namedoutput");
    if (outputName != null && !outputName.isEmpty()) {
      schema = (new Schema.Parser()).parse(conf.get("avro.output.schema." + outputName));
    } else {
      schema = AvroJob.getOutputSchema(conf);
    }

    DataFileWriter<S> writer = new DataFileWriter<S>(AvroMode.fromConfiguration(conf).<S>getWriter(schema));

    JobConf jc = new JobConf(conf);
    /* copied from org.apache.avro.mapred.AvroOutputFormat */

    if (org.apache.hadoop.mapred.FileOutputFormat.getCompressOutput(jc)) {
      int level = conf.getInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
          org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL);
      String codecName = conf.get(AvroJob.OUTPUT_CODEC,
          org.apache.avro.file.DataFileConstants.DEFLATE_CODEC);
      CodecFactory codec = codecName.equals(org.apache.avro.file.DataFileConstants.DEFLATE_CODEC)
          ? CodecFactory.deflateCodec(level)
          : CodecFactory.fromString(codecName);
      writer.setCodec(codec);
    }

    writer.setSyncInterval(jc.getInt(org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY,
        org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL));

    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      writer.create(schema, fs.append(path));
    } else {
      writer.create(schema, fs.create(path));
    }
    return writer;
  }

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {

    Configuration conf = context.getConfiguration();
    Path path = getDefaultWorkFile(context, org.apache.avro.mapred.AvroOutputFormat.EXT);
    final DataFileWriter<T> writer = getDataFileWriter(path, conf);

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      @Override
      public void write(AvroWrapper<T> wrapper, NullWritable ignore) throws IOException {
        writer.append(wrapper.datum());
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }

}
