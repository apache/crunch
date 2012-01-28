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
package com.cloudera.crunch.io.text;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.MapReduceTarget;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.Converter;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;

public class TextFileTarget implements PathTarget, MapReduceTarget {

  protected final Path path;
  
  public TextFileTarget(String path) {
    this(new Path(path));
  }
  
  public TextFileTarget(Path path) {
    this.path = path;
  }
  
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TextFileTarget)) {
      return false;
    }
    TextFileTarget o = (TextFileTarget) other;
    return path.equals(o.path);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).toHashCode();
  }
  
  @Override
  public String toString() {
    return "TextTarget(" + path + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Converter converter = ptype.getConverter();
    SourceTargetHelper.configureTarget(job, TextOutputFormat.class,
        converter.getKeyClass(), converter.getValueClass(), outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof PTableType) {
      return null;
    }
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
