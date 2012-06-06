/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.io.impl;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PType;

public class FileTargetImpl implements PathTarget {

  protected final Path path;
  private final Class<? extends FileOutputFormat> outputFormatClass;
  
  public FileTargetImpl(Path path, Class<? extends FileOutputFormat> outputFormatClass) {
	this.path = path;
	this.outputFormatClass = outputFormatClass;
  }
  
  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath,
	  String name) {
    Converter converter = ptype.getConverter();
    Class keyClass = converter.getKeyClass();
    Class valueClass = converter.getValueClass();
    configureForMapReduce(job, keyClass, valueClass, outputPath, name);
  }

  protected void configureForMapReduce(Job job, Class keyClass, Class valueClass,
	  Path outputPath, String name) {
    try {
      FileOutputFormat.setOutputPath(job, outputPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (name == null) {
      job.setOutputFormatClass(outputFormatClass);
      job.setOutputKeyClass(keyClass);
      job.setOutputValueClass(valueClass);
    } else {
      CrunchMultipleOutputs.addNamedOutput(job, name, outputFormatClass,
          keyClass, valueClass);
    }	
  }
  
  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }
  
  @Override
  public Path getPath() {
	return path;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    FileTargetImpl o = (FileTargetImpl) other;
    return path.equals(o.path);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).toHashCode();
  }
  
  @Override
  public String toString() {
	return new StringBuilder().append(outputFormatClass.getSimpleName())
	    .append("(").append(path).append(")").toString();
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
	// By default, assume that we cannot do this.
	return null;
  }
}
