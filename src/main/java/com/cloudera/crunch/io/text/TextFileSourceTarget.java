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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.writable.Writables;
import com.google.common.collect.UnmodifiableIterator;

public class TextFileSourceTarget extends TextFileTarget implements ReadableSourceTarget<String> {

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
    return "TextFileSourceTarget(" + path + ")";
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

  @Override
  public Iterable<String> read(Configuration conf) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	FSDataInputStream is = fs.open(path);
	BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	return new BRIterable(reader);
  }

  private static class BRIterable implements Iterable<String> {
	private final BufferedReader reader;
	
	public BRIterable(BufferedReader reader) {
	  this.reader = reader;
	}
	
	@Override
	public Iterator<String> iterator() {
	  return new UnmodifiableIterator<String>() {
		private String nextLine;
		@Override
		public boolean hasNext() {
		  try {
			return (nextLine = reader.readLine()) != null;
		  } catch (IOException e) {
			LOG.info("Exception reading text file stream", e);
			return false;
		  }
		}

		@Override
		public String next() {
		  return nextLine;
		}
	  };
	}
  }
}
