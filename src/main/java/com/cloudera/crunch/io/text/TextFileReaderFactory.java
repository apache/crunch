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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.fn.CompositeMapFn;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.io.FileReaderFactory;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

public class TextFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(TextFileReaderFactory.class);
  
  private final PType<T> ptype;
  private final Configuration conf;
  
  public TextFileReaderFactory(PType<T> ptype, Configuration conf) {
    this.ptype = ptype;
    this.conf = conf;
  }
  
  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    MapFn mapFn = null;
    if (String.class.equals(ptype.getTypeClass())) {
      mapFn = IdentityFn.getInstance();
    } else {
      // Check for a composite MapFn for the PType.
      // Note that this won't work for Avro-- need to solve that.
      MapFn input = ptype.getInputMapFn();
      if (input instanceof CompositeMapFn) {
        mapFn = ((CompositeMapFn) input).getSecond();
      }
    }
    mapFn.setConfigurationForTest(conf);
    mapFn.initialize();
    
	FSDataInputStream is = null;
	try {
	  is = fs.open(path);
	} catch (IOException e) {
	  LOG.info("Could not read path: " + path, e);
	  return Iterators.emptyIterator();
	}
	
	final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	final MapFn<String, T> iterMapFn = mapFn;
	return new UnmodifiableIterator<T>() {
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
	  public T next() {
		return iterMapFn.map(nextLine);
	  }
	};
  }
}
