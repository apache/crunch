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

import org.apache.avro.mapred.AvroWrapper;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroOutputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

public class AvroFileTarget extends FileTargetImpl {
  public AvroFileTarget(String path) {
    this(new Path(path));
  }

  public AvroFileTarget(Path path) {
    super(path, AvroOutputFormat.class);
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
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType<?>) ptype;
    Configuration conf = job.getConfiguration();
    String schemaParam = null;
    if (name == null) {
      schemaParam = "avro.output.schema";
    } else {
      schemaParam = "avro.output.schema." + name;
    }
    String outputSchema = conf.get(schemaParam);
    if (outputSchema == null) {
      conf.set(schemaParam, atype.getSchema().toString());
    } else if (!outputSchema.equals(atype.getSchema().toString())) {
      throw new IllegalStateException("Avro targets must use the same output schema");
    }
    Avros.configureReflectDataFactory(conf);
    configureForMapReduce(job, AvroWrapper.class, NullWritable.class, outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof AvroType) {
      return new AvroFileSourceTarget<T>(path, (AvroType<T>) ptype);
    }
    return null;
  }
}
