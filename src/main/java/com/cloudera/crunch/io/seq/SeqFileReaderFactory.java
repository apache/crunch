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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.io.FileReaderFactory;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

public class SeqFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(SeqFileReaderFactory.class);
  
  private final MapFn<Object, T> mapFn;
  private final Writable key;
  private final Writable value;
  private final Configuration conf;

  public SeqFileReaderFactory(PType<T> ptype, Configuration conf) {
	this.mapFn = SeqFileHelper.getInputMapFn(ptype);
	this.key = NullWritable.get();
	this.value = SeqFileHelper.newInstance(ptype, conf);
	this.conf = conf;
  }
  
  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {
    mapFn.setConfigurationForTest(conf);
    mapFn.initialize();
	try {
	  final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
	  return new UnmodifiableIterator<T>() {
	    boolean nextChecked = false;
	    boolean hasNext = false;

	    @Override
		public boolean hasNext() {
		  if (nextChecked == true) {
		    return hasNext;
		  }
		  try {
			hasNext = reader.next(key, value);
			nextChecked = true;
			return hasNext;
		  } catch (IOException e) {
			LOG.info("Error reading from path: " + path, e);
			return false;
		  }
		}

		@Override
		public T next() {
		  if (!nextChecked && !hasNext()) {
		    return null;
		  }
		  nextChecked = false;
		  return mapFn.map(value);
		}
	  };
	} catch (IOException e) {
	  LOG.info("Could not read seqfile at path: " + path, e);
	  return Iterators.emptyIterator();
	}
  }

}
