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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.cloudera.crunch.io.CompositePathIterable;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.avro.AvroTypeFamily;
import com.cloudera.crunch.type.avro.AvroUtf8InputFormat;

public class TextFileSourceTarget<T> extends TextFileTarget implements ReadableSourceTarget<T> {

  private static final Log LOG = LogFactory.getLog(TextFileSourceTarget.class);
  
  private final PType<T> ptype;
  
  public TextFileSourceTarget(String path, PType<T> ptype) {
    this(new Path(path), ptype);
  }
  
  public TextFileSourceTarget(Path path, PType<T> ptype) {
    super(path);
    this.ptype = ptype;
  }
  
  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TextFileSourceTarget)) {
      return false;
    }
    TextFileSourceTarget o = (TextFileSourceTarget) other;
    return path.equals(o.path) && ptype.equals(o.ptype);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).toHashCode();
  }
  
  @Override
  public String toString() {
    return "TextFile(" + path + ")";
  }
  
  private boolean isBZip2() {
    String strPath = path.toString();
    return strPath.endsWith(".bz") || strPath.endsWith(".bz2");
  }
  
  @Override
  public void configureSource(Job job, int inputId) throws IOException {
	if (ptype.getFamily().equals(AvroTypeFamily.getInstance())) {
      SourceTargetHelper.configureSource(job, inputId, AvroUtf8InputFormat.class, path);
	} else {
	  if (isBZip2()) {
	    SourceTargetHelper.configureSource(job, inputId, BZip2TextInputFormat.class, path);
	  } else {
        SourceTargetHelper.configureSource(job, inputId, TextInputFormat.class, path);
	  }
	}
  }

  @Override
  public long getSize(Configuration conf) {
    try {
      long sz = SourceTargetHelper.getPathSize(conf, path);
      if (isBZip2()) {
        sz *= 10; // Arbitrary compression factor
      }
      return sz;
    } catch (IOException e) {
      LOG.info(String.format("Exception thrown looking up size of: %s", path), e);
    }
    return 1L;
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
	return CompositePathIterable.create(FileSystem.get(conf), path,
	    new TextFileReaderFactory<T>(ptype));
  }
}
