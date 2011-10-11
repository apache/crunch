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

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.writable.Writables;

public class TextFileSourceTarget extends TextFileTarget implements SourceTarget<String> {

  private static final Log LOG = LogFactory.getLog(TextFileSourceTarget.class);
  
  public TextFileSourceTarget(String path) {
    this(new Path(path));
  }
  
  public TextFileSourceTarget(Path path) {
    super(path);
  }
  
  @Override
  public PType<String> getType() {
    return Writables.strings();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TextFileSourceTarget)) {
      return false;
    }
    TextFileSourceTarget o = (TextFileSourceTarget) other;
    return path.equals(o.path);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).toHashCode();
  }
  
  @Override
  public String toString() {
    return "TextSourceTarget(" + path + ")";
  }
  
  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    SourceTargetHelper.configureSource(job, inputId, TextInputFormat.class, path);
  }

  @Override
  public long getSize(Configuration conf) {
    try {
      return SourceTargetHelper.getPathSize(conf, path);
    } catch (IOException e) {
      LOG.info(String.format("Exception thrown looking up size of: %s", path), e);
    }
    return 1L;
  }

}
