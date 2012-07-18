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
package org.apache.crunch.io;

import org.apache.crunch.types.PType;
import org.apache.crunch.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class PathTargetImpl implements PathTarget {

  private final Path path;
  private final Class<OutputFormat> outputFormatClass;
  private final Class keyClass;
  private final Class valueClass;

  public PathTargetImpl(String path, Class<OutputFormat> outputFormatClass, Class keyClass, Class valueClass) {
    this(new Path(path), outputFormatClass, keyClass, valueClass);
  }

  public PathTargetImpl(Path path, Class<OutputFormat> outputFormatClass, Class keyClass, Class valueClass) {
    this.path = path;
    this.outputFormatClass = outputFormatClass;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    try {
      FileOutputFormat.setOutputPath(job, path);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (name == null) {
      job.setOutputFormatClass(outputFormatClass);
      job.setOutputKeyClass(keyClass);
      job.setOutputValueClass(valueClass);
    } else {
      CrunchMultipleOutputs.addNamedOutput(job, name, outputFormatClass, keyClass, valueClass);
    }
  }

  @Override
  public Path getPath() {
    return path;
  }
}
