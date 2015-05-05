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
package org.apache.crunch.io.avro;

import com.google.common.collect.Maps;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroOutputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

public class AvroFileTarget extends FileTargetImpl {

  private Map<String, String> extraConf = Maps.newHashMap();

  public AvroFileTarget(String path) {
    this(new Path(path));
  }

  public AvroFileTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public AvroFileTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, AvroOutputFormat.class, fileNamingScheme);
  }

  @Override
  public String toString() {
    return "Avro(" + path.toString() + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public Target outputConf(String key, String value) {
    extraConf.put(key, value);
    return this;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType<?>) ptype;
    FormatBundle bundle = FormatBundle.forOutput(AvroOutputFormat.class);
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      bundle.set(e.getKey(), e.getValue());
    }
    bundle.set("avro.output.schema", atype.getSchema().toString());
    AvroMode.fromType(atype).configure(bundle);
    configureForMapReduce(job, AvroWrapper.class, NullWritable.class, bundle,
        outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof AvroType) {
      return new AvroFileSourceTarget<T>(path, (AvroType<T>) ptype);
    }
    return null;
  }
}
