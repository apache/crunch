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
package com.cloudera.crunch.io.seq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.cloudera.crunch.io.CompositePathIterable;
import com.cloudera.crunch.io.ReadableSource;
import com.cloudera.crunch.io.impl.FileSourceImpl;
import com.cloudera.crunch.types.PType;

public class SeqFileSource<T> extends FileSourceImpl<T> implements
	ReadableSource<T> {

  public SeqFileSource(Path path, PType<T> ptype) {
	super(path, ptype, SequenceFileInputFormat.class);
  }
  
  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	return CompositePathIterable.create(fs, path, 
	    new SeqFileReaderFactory<T>(ptype, conf));
  }

  @Override
  public String toString() {
    return "SeqFile(" + path.toString() + ")";
  }
}
