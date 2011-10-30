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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.cloudera.crunch.Source;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.io.avro.AvroFileSourceTarget;
import com.cloudera.crunch.io.hbase.HBaseSourceTarget;
import com.cloudera.crunch.io.seq.SeqFileSourceTarget;
import com.cloudera.crunch.io.seq.SeqFileTableSourceTarget;
import com.cloudera.crunch.io.text.TextFileSourceTarget;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.avro.AvroType;
import com.cloudera.crunch.type.writable.Writables;

/**
 * Static factory methods for creating various {@link Source} types.
 *
 */
public class From {

  public static <T> Source<T> avroFile(String pathName, AvroType<T> avroType) {
	return avroFile(new Path(pathName), avroType);
  }
  
  public static <T> Source<T> avroFile(Path path, AvroType<T> avroType) {
	return new AvroFileSourceTarget<T>(path, avroType);
  }
  
  public static TableSource<ImmutableBytesWritable, Result> hbaseTable(String table) {
	return hbaseTable(table, new Scan());
  }
  
  public static TableSource<ImmutableBytesWritable, Result> hbaseTable(String table, Scan scan) {
	return new HBaseSourceTarget(table, scan);
  }
  
  public static <T> Source<T> sequenceFile(String pathName, PType<T> ptype) {
	return sequenceFile(new Path(pathName), ptype);
  }
  
  public static <T> Source<T> sequenceFile(Path path, PType<T> ptype) {
	return new SeqFileSourceTarget<T>(path, ptype);
  }
  
  public static <K, V> TableSource<K, V> sequenceFile(String pathName, PType<K> keyType,
      PType<V> valueType) {
	return sequenceFile(new Path(pathName), keyType, valueType);
  }
  
  public static <K, V> TableSource<K, V> sequenceFile(Path path, PType<K> keyType,
      PType<V> valueType) {
	PTypeFamily ptf = keyType.getFamily();
	return new SeqFileTableSourceTarget<K, V>(path, ptf.tableOf(keyType, valueType));
  }
  
  public static Source<String> textFile(String pathName) {
	return textFile(new Path(pathName));
  }
  
  public static Source<String> textFile(Path path) {
	return textFile(path, Writables.strings());
  }
  
  public static <T> Source<T> textFile(String pathName, PType<T> ptype) {
    return textFile(new Path(pathName), ptype);
  }
  
  public static <T> Source<T> textFile(Path path, PType<T> ptype) {
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
