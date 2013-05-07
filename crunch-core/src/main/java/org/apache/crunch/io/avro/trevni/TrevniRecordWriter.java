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
package org.apache.crunch.io.avro.trevni;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.MetaData;
import org.apache.trevni.avro.AvroColumnWriter;

/**
 *
 */
public class TrevniRecordWriter<T> extends RecordWriter<AvroKey<T>, NullWritable> {

  /** trevni file extension */
  public final static String EXT = ".trv";
  
  /** prefix of job configs that we care about */
  public static final String META_PREFIX = "trevni.meta.";
  
  /** Counter that increments as new trevni files are create because the current file 
   * has exceeded the block size 
   * */
  protected int part = 0;

  /** Trevni file writer */
  protected AvroColumnWriter<T> writer;

  /** This will be a unique directory linked to the task */
  final Path dirPath;
  
  /** HDFS object */
  final FileSystem fs;

  /** Current configured blocksize */
  final long blockSize;
  
  /** Provided avro schema from the context */
  protected Schema schema;
  
  /** meta data to be stored in the output file.  */
  protected ColumnFileMetaData meta;
  
  public TrevniRecordWriter(TaskAttemptContext context) throws IOException {
    schema = initSchema(context);
    meta = filterMetadata(context.getConfiguration());
    writer = new AvroColumnWriter<T>(schema, meta, ReflectData.get());

    Path outputPath = FileOutputFormat.getOutputPath(context);
    
    String dir = FileOutputFormat.getUniqueFile(context, "part", "");
    dirPath = new Path(outputPath.toString() + "/" + dir);
    fs = dirPath.getFileSystem(context.getConfiguration());
    fs.mkdirs(dirPath);

    blockSize = fs.getDefaultBlockSize();
  }

  /** {@inheritDoc} */
  @Override
  public void write(AvroKey<T> key, NullWritable value) throws IOException,
      InterruptedException {
    writer.write(key.datum());
    if (writer.sizeEstimate() >= blockSize) // block full
      flush();
  }

  /** {@inheritDoc} */
  protected Schema initSchema(TaskAttemptContext context) {
    boolean isMapOnly = context.getNumReduceTasks() == 0;
    return isMapOnly ? AvroJob.getMapOutputKeySchema(context
        .getConfiguration()) : AvroJob.getOutputKeySchema(context
        .getConfiguration());
  }
  
  /**
   * A Trevni flush will close the current file and prep a new writer
   * @throws IOException
   */
  public void flush() throws IOException {
    OutputStream out = fs.create(new Path(dirPath, "part-" + (part++) + EXT));
    try {
      writer.writeTo(out);
    } finally {
      out.close();
    }
    writer = new AvroColumnWriter<T>(schema, meta, ReflectData.get());
  }
  
  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext arg0) throws IOException,
      InterruptedException {
    flush();
  }
  
  static ColumnFileMetaData filterMetadata(final Configuration configuration) {
    final ColumnFileMetaData meta = new ColumnFileMetaData();
    Iterator<Entry<String, String>> keyIterator = configuration.iterator();

    while (keyIterator.hasNext()) {
      Entry<String, String> confEntry = keyIterator.next();
      if (confEntry.getKey().startsWith(META_PREFIX))
        meta.put(confEntry.getKey().substring(META_PREFIX.length()), confEntry
            .getValue().getBytes(MetaData.UTF8));
    }

    return meta;
  }
}
