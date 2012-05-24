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

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.Source;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.types.PType;

public class SourceTargetImpl<T> implements SourceTarget<T> {

  protected final Source<T> source;
  protected final Target target;
  
  public SourceTargetImpl(Source<T> source, Target target) {
	this.source = source;
	this.target = target;
  }
  
  @Override
  public PType<T> getType() {
	return source.getType();
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
	source.configureSource(job, inputId);
  }

  @Override
  public long getSize(Configuration configuration) {
	return source.getSize(configuration);
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
	return target.accept(handler, ptype);
  }

  @Override
  public <S> SourceTarget<S> asSourceTarget(PType<S> ptype) {
	return target.asSourceTarget(ptype);
  }
  
  @Override
  public boolean equals(Object other) {
	if (other == null || !(other.getClass().equals(getClass()))) {
	  return false;
	}
	SourceTargetImpl sti = (SourceTargetImpl) other;
	return source.equals(sti.source) && target.equals(sti.target);
  }
  
  @Override
  public int hashCode() {
	return new HashCodeBuilder().append(source).append(target).toHashCode();
  }
  
  @Override
  public String toString() {
	return source.toString();
  }
}
