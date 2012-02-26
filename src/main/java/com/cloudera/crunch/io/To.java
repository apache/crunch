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
package com.cloudera.crunch.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.crunch.Target;
import com.cloudera.crunch.io.avro.AvroFileTarget;
import com.cloudera.crunch.io.hbase.HBaseTarget;
import com.cloudera.crunch.io.impl.FileTargetImpl;
import com.cloudera.crunch.io.seq.SeqFileTarget;
import com.cloudera.crunch.io.text.TextFileTarget;

/**
 * Static factory methods for creating various {@link Target} types.
 *
 */
public class To {
  
  public static Target formattedFile(String pathName, Class<? extends FileOutputFormat> formatClass) {
	return formattedFile(new Path(pathName), formatClass);
  }
  
  public static Target formattedFile(Path path, Class<? extends FileOutputFormat> formatClass) {
	return new FileTargetImpl(path, formatClass);
  }
  
  public static Target avroFile(String pathName) {
	return avroFile(new Path(pathName));
  }
  
  public static Target avroFile(Path path) {
	return new AvroFileTarget(path);
  }
  
  public static Target hbaseTable(String table) {
	return new HBaseTarget(table);
  }
  
  public static Target sequenceFile(String pathName) {
	return sequenceFile(new Path(pathName));
  }
  
  public static Target sequenceFile(Path path) {
	return new SeqFileTarget(path);
  }
  
  public static Target textFile(String pathName) {
	return textFile(new Path(pathName));
  }
  
  public static Target textFile(Path path) {
	return new TextFileTarget(path);
  }  

}
