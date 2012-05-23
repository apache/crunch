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
import org.apache.hadoop.hbase.client.Scan;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.avro.AvroFileSourceTarget;
import com.cloudera.crunch.io.hbase.HBaseSourceTarget;
import com.cloudera.crunch.io.seq.SeqFileSourceTarget;
import com.cloudera.crunch.io.seq.SeqFileTableSourceTarget;
import com.cloudera.crunch.io.text.TextFileSourceTarget;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroType;
import com.cloudera.crunch.types.writable.Writables;

/**
 * Static factory methods for creating various {@link SourceTarget} types.
 *
 */
public class At {
  public static <T> AvroFileSourceTarget<T> avroFile(String pathName, AvroType<T> avroType) {
	return avroFile(new Path(pathName), avroType);
  }
  
  public static <T> AvroFileSourceTarget<T> avroFile(Path path, AvroType<T> avroType) {
	return new AvroFileSourceTarget<T>(path, avroType);
  }
  
  public static HBaseSourceTarget hbaseTable(String table) {
	return hbaseTable(table, new Scan());
  }
  
  public static HBaseSourceTarget hbaseTable(String table, Scan scan) {
	return new HBaseSourceTarget(table, scan);
  }
  
  public static <T> SeqFileSourceTarget<T> sequenceFile(String pathName, PType<T> ptype) {
	return sequenceFile(new Path(pathName), ptype);
  }
  
  public static <T> SeqFileSourceTarget<T> sequenceFile(Path path, PType<T> ptype) {
	return new SeqFileSourceTarget<T>(path, ptype);
  }
  
  public static <K, V> SeqFileTableSourceTarget<K, V> sequenceFile(String pathName, PType<K> keyType,
      PType<V> valueType) {
	return sequenceFile(new Path(pathName), keyType, valueType);
  }
  
  public static <K, V> SeqFileTableSourceTarget<K, V> sequenceFile(Path path, PType<K> keyType,
      PType<V> valueType) {
	PTypeFamily ptf = keyType.getFamily();
	return new SeqFileTableSourceTarget<K, V>(path, ptf.tableOf(keyType, valueType));
  }
  
  public static TextFileSourceTarget<String> textFile(String pathName) {
	return textFile(new Path(pathName));
  }
  
  public static TextFileSourceTarget<String> textFile(Path path) {
	return textFile(path, Writables.strings());
  }
  
  public static <T> TextFileSourceTarget<T> textFile(String pathName, PType<T> ptype) {
    return textFile(new Path(pathName), ptype);
  }
  
  public static <T> TextFileSourceTarget<T> textFile(Path path, PType<T> ptype) {
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
