package com.cloudera.crunch.io.impl;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.type.Converter;
import com.cloudera.crunch.type.PType;

public abstract class FileTargetImpl implements PathTarget {

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
    FileOutputFormat.setOutputPath(job, outputPath);
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
}
