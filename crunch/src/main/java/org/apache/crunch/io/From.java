/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io;

import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.io.seq.SeqFileSource;
import org.apache.crunch.io.seq.SeqFileTableSourceTarget;
import org.apache.crunch.io.text.TextFileSource;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Static factory methods for creating various {@link Source} types.
 * 
 */
public class From {

  public static <K, V> TableSource<K, V> formattedFile(String path, Class<? extends FileInputFormat> formatClass,
      PType<K> keyType, PType<V> valueType) {
    return formattedFile(new Path(path), formatClass, keyType, valueType);
  }

  public static <K, V> TableSource<K, V> formattedFile(Path path, Class<? extends FileInputFormat> formatClass,
      PType<K> keyType, PType<V> valueType) {
    PTableType<K, V> tableType = keyType.getFamily().tableOf(keyType, valueType);
    return new FileTableSourceImpl<K, V>(path, tableType, formatClass);
  }

  public static <T> Source<T> avroFile(String pathName, AvroType<T> avroType) {
    return avroFile(new Path(pathName), avroType);
  }

  public static <T> Source<T> avroFile(Path path, AvroType<T> avroType) {
    return new AvroFileSource<T>(path, avroType);
  }

  public static <T> Source<T> sequenceFile(String pathName, PType<T> ptype) {
    return sequenceFile(new Path(pathName), ptype);
  }

  public static <T> Source<T> sequenceFile(Path path, PType<T> ptype) {
    return new SeqFileSource<T>(path, ptype);
  }

  public static <K, V> TableSource<K, V> sequenceFile(String pathName, PType<K> keyType, PType<V> valueType) {
    return sequenceFile(new Path(pathName), keyType, valueType);
  }

  public static <K, V> TableSource<K, V> sequenceFile(Path path, PType<K> keyType, PType<V> valueType) {
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
    return new TextFileSource<T>(path, ptype);
  }
}
