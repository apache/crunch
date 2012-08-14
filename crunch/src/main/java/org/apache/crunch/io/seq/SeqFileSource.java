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

import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class SeqFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  public SeqFileSource(Path path, PType<T> ptype) {
    super(path, ptype, SequenceFileInputFormat.class);
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return CompositePathIterable.create(fs, path, new SeqFileReaderFactory<T>(ptype, conf));
  }

  @Override
  public String toString() {
    return "SeqFile(" + path.toString() + ")";
  }
}
