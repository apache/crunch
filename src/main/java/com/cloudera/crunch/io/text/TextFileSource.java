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
package com.cloudera.crunch.io.text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.cloudera.crunch.io.CompositePathIterable;
import com.cloudera.crunch.io.ReadableSource;
import com.cloudera.crunch.io.impl.FileSourceImpl;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.avro.AvroUtf8InputFormat;

public class TextFileSource<T> extends FileSourceImpl<T> implements
	ReadableSource<T> {

  private static boolean isBZip2(Path path) {
	String strPath = path.toString();
	return strPath.endsWith(".bz") || strPath.endsWith(".bz2");
  }
  
  private static Class<? extends FileInputFormat> getInputFormat(Path path, PType ptype) {
	if (ptype.getFamily().equals(AvroTypeFamily.getInstance())) {
	  return AvroUtf8InputFormat.class;
	} else if (isBZip2(path)){
      return BZip2TextInputFormat.class;
	} else {
  	  return TextInputFormat.class;
	}
  }
  
  public TextFileSource(Path path, PType<T> ptype) {
	super(path, ptype, getInputFormat(path, ptype));
  }
  
  @Override
  public long getSize(Configuration conf) {
	long sz = super.getSize(conf);
	if (isBZip2(path)) {
	  sz *= 10; // Arbitrary compression factor
	}
	return sz;
  }
  
  @Override
  public String toString() {
    return "Text(" + path + ")";
  }
  
  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
	return CompositePathIterable.create(FileSystem.get(conf), path,
	    new TextFileReaderFactory<T>(ptype, conf));
  }
}
