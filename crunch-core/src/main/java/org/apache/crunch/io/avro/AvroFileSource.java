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
package org.apache.crunch.io.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroJob;
import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  private static <S> FormatBundle getBundle(AvroType<S> ptype) {
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class)
        .set(AvroJob.INPUT_IS_REFLECT, String.valueOf(ptype.hasReflect()))
        .set(AvroJob.INPUT_SCHEMA, ptype.getSchema().toString())
        .set(Avros.REFLECT_DATA_FACTORY_CLASS, Avros.REFLECT_DATA_FACTORY.getClass().getName());
    return bundle;
  }
  
  public AvroFileSource(Path path, AvroType<T> ptype) {
    super(path, ptype, getBundle(ptype));
  }

  @Override
  public String toString() {
    return "Avro(" + path.toString() + ")";
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return CompositePathIterable.create(fs, path, new AvroFileReaderFactory<T>((AvroType<T>) ptype));
  }
}
