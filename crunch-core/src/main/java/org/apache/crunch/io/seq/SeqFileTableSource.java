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
package org.apache.crunch.io.seq;

import java.io.IOException;

import java.util.List;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * A {@code TableSource} that uses {@code SequenceFileInputFormat} to read the input
 * file.
 */
public class SeqFileTableSource<K, V> extends FileTableSourceImpl<K, V> implements ReadableSource<Pair<K, V>> {

  public SeqFileTableSource(String path, PTableType<K, V> ptype) {
    this(new Path(path), ptype);
  }

  public SeqFileTableSource(Path path, PTableType<K, V> ptype) {
    super(path, ptype, SequenceFileInputFormat.class);
  }

  public SeqFileTableSource(List<Path> paths, PTableType<K, V> ptype) {
    super(paths, ptype, SequenceFileInputFormat.class);
  }

  @Override
  public Iterable<Pair<K, V>> read(Configuration conf) throws IOException {
    return read(conf, new SeqFileReaderFactory<Pair<K, V>>(getTableType()));
  }

  @Override
  public ReadableData<Pair<K, V>> asReadable() {
    return new SeqFileReadableData<Pair<K, V>>(paths, getTableType());
  }

  @Override
  public String toString() {
    return "SeqFile(" + pathsAsString() + ")";
  }
}
