package com.cloudera.crunch.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.crunch.types.PType;

public abstract class PathTargetImpl implements PathTarget {

  private final Path path;
  private final Class<OutputFormat> outputFormatClass;
  private final Class keyClass;
  private final Class valueClass;
  
  public PathTargetImpl(String path, Class<OutputFormat> outputFormatClass,
	  Class keyClass, Class valueClass) {
	this(new Path(path), outputFormatClass, keyClass, valueClass);
  }
  
  public PathTargetImpl(Path path, Class<OutputFormat> outputFormatClass,
	  Class keyClass, Class valueClass) {
	this.path = path;
	this.outputFormatClass = outputFormatClass;
	this.keyClass = keyClass;
	this.valueClass = valueClass;
  }
  
  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath,
	  String name) {
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
      CrunchMultipleOutputs.addNamedOutput(job, name, outputFormatClass,
          keyClass, valueClass);
    }
  }

  @Override
  public Path getPath() {
	return path;
  }
}
