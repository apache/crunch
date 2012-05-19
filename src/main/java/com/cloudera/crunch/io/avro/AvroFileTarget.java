/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.io.avro;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.io.impl.FileTargetImpl;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.avro.AvroOutputFormat;
import com.cloudera.crunch.types.avro.AvroType;
import com.cloudera.crunch.types.avro.Avros;

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
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath,
      String name) {
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
    configureForMapReduce(job, AvroWrapper.class, NullWritable.class,
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
