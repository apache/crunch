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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.io.FileReaderFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

public class TextFileReaderFactory implements FileReaderFactory<String> {

  private static final Log LOG = LogFactory.getLog(TextFileReaderFactory.class);
  
  @Override
  public Iterator<String> read(FileSystem fs, Path path) {
	FSDataInputStream is = null;
	try {
	  is = fs.open(path);
	} catch (IOException e) {
	  LOG.info("Could not read path: " + path, e);
	  return Iterators.emptyIterator();
	}
	
	final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
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
