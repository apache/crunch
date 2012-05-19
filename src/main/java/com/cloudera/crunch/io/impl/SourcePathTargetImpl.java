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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.Source;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.types.PType;

public class SourcePathTargetImpl<T> extends SourceTargetImpl<T> implements
	PathTarget {

  public SourcePathTargetImpl(Source<T> source, PathTarget target) {
	super(source, target);
  }
  
  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath,
	  String name) {
	((PathTarget) target).configureForMapReduce(job, ptype, outputPath, name);
  }

  @Override
  public Path getPath() {
	return ((PathTarget) target).getPath();
  }
}
