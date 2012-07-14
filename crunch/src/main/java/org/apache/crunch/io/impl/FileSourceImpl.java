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
package org.apache.crunch.io.impl;

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.run.CrunchInputs;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class FileSourceImpl<T> implements Source<T> {

  private static final Log LOG = LogFactory.getLog(FileSourceImpl.class);

  protected final Path path;
  protected final PType<T> ptype;
  protected final InputBundle inputBundle;

  public FileSourceImpl(Path path, PType<T> ptype, Class<? extends InputFormat> inputFormatClass) {
    this.path = path;
    this.ptype = ptype;
    this.inputBundle = new InputBundle(inputFormatClass);
  }

  public FileSourceImpl(Path path, PType<T> ptype, InputBundle inputBundle) {
    this.path = path;
    this.ptype = ptype;
    this.inputBundle = inputBundle;
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    if (inputId == -1) {
      FileInputFormat.addInputPath(job, path);
      job.setInputFormatClass(inputBundle.getInputFormatClass());
      inputBundle.configure(job.getConfiguration());
    } else {
      CrunchInputs.addInputPath(job, path, inputBundle, inputId);
    }
  }

  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public long getSize(Configuration configuration) {
    try {
      return SourceTargetHelper.getPathSize(configuration, path);
    } catch (IOException e) {
      LOG.warn(String.format("Exception thrown looking up size of: %s", path), e);
      throw new IllegalStateException("Failed to get the file size of:" + path, e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    FileSourceImpl o = (FileSourceImpl) other;
    return ptype.equals(o.ptype) && path.equals(o.path) && inputBundle.equals(o.inputBundle);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(ptype).append(path).append(inputBundle).toHashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(inputBundle.getName()).append("(").append(path).append(")").toString();
  }
}
