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
package org.apache.crunch.io.text;

import java.io.IOException;

import java.util.List;
import org.apache.crunch.Pair;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

/** 
 * A {@code Source} that uses the {@code KeyValueTextInputFormat} to process
 * input text. If a separator for the keys and values in the text file is not specified,
 * a tab character is used. 
 */
public class TextFileTableSource<K, V> extends FileTableSourceImpl<K, V>
    implements ReadableSource<Pair<K, V>> {

  // CRUNCH-125: Maintain compatibility with both versions of the KeyValueTextInputFormat's
  // configuration field for specifying the separator character.
  private static final String OLD_KV_SEP = "key.value.separator.in.input.line";
  private static final String NEW_KV_SEP = "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
  
  private static FormatBundle getBundle(String sep) {
    FormatBundle bundle = FormatBundle.forInput(KeyValueTextInputFormat.class);
    bundle.set(OLD_KV_SEP, sep);
    bundle.set(NEW_KV_SEP, sep);
    return bundle;
  }
  
  private final String separator;
  
  public TextFileTableSource(String path, PTableType<K, V> tableType) {
    this(new Path(path), tableType);
  }
  
  public TextFileTableSource(Path path, PTableType<K, V> tableType) {
    this(path, tableType, "\t");
  }

  public TextFileTableSource(List<Path> paths, PTableType<K, V> tableType) {
    this(paths, tableType, "\t");
  }
  
  public TextFileTableSource(String path, PTableType<K, V> tableType, String separator) {
    this(new Path(path), tableType, separator);
  }
  
  public TextFileTableSource(Path path, PTableType<K, V> tableType, String separator) {
    super(path, tableType, getBundle(separator));
    this.separator = separator;
  }

  public TextFileTableSource(List<Path> paths, PTableType<K, V> tableType, String separator) {
    super(paths, tableType, getBundle(separator));
    this.separator = separator;
  }

  @Override
  public String toString() {
    return "KeyValueText(" + pathsAsString() + ")";
  }

  @Override
  public Iterable<Pair<K, V>> read(Configuration conf) throws IOException {
    return read(conf,
        new TextFileReaderFactory<Pair<K, V>>(LineParser.forTableType(getTableType(),
            separator)));
  }

  @Override
  public ReadableData<Pair<K, V>> asReadable() {
    return new TextReadableData<Pair<K, V>>(paths, getTableType(), separator);
  }
}
