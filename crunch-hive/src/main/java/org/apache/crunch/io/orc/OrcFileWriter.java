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

import java.io.Closeable;
import java.io.IOException;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * A writer class which is corresponding to OrcFileReaderFactory. Mainly used
 * for test purpose
 *
 * @param <T>
 */
public class OrcFileWriter<T> implements Closeable {
  
  private RecordWriter<NullWritable, Object> writer;
  private MapFn<T, Object> mapFn;
  private final OrcSerde serde;
  
  static class NullProgress implements Progressable {
    @Override
    public void progress() {
    }
  }
  
  public OrcFileWriter(Configuration conf, Path path, PType<T> pType) throws IOException {
    JobConf jobConf = new JobConf(conf);
    OutputFormat outputFormat = new OrcOutputFormat();
    writer = outputFormat.getRecordWriter(null, jobConf, path.toString(), new NullProgress());
    
    mapFn = pType.getOutputMapFn();
    mapFn.initialize();
    
    serde = new OrcSerde();
  }
  
  public void write(T t) throws IOException {
    OrcWritable ow = (OrcWritable) mapFn.map(t);
    if (ow.get() == null) {
      throw new NullPointerException("Cannot write null records to orc file");
    }
    writer.write(NullWritable.get(), serde.serialize(ow.get(), ow.getObjectInspector()));
  }

  @Override
  public void close() throws IOException {
    writer.close(Reporter.NULL);
  }

}
