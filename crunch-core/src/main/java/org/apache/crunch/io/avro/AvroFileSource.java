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

import java.util.List;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AvroFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  private static <S> FormatBundle getBundle(AvroType<S> ptype, AvroMode mode) {
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class)
          .set(AvroJob.INPUT_IS_REFLECT, String.valueOf(ptype.hasReflect()))
          .set(AvroJob.INPUT_SCHEMA, ptype.getSchema().toString())
          .set(Avros.REFLECT_DATA_FACTORY_CLASS, Avros.REFLECT_DATA_FACTORY.getClass().getName())
          .set(RuntimeParameters.DISABLE_COMBINE_FILE, Boolean.FALSE.toString());
    mode.configure(bundle);
    return bundle;
  }


  private static <S> FormatBundle getBundle(AvroType<S> ptype) {
    return getBundle(ptype, AvroMode.fromType(ptype));
  }

  private DatumReader<T> reader;
  
  public AvroFileSource(Path path, AvroType<T> ptype) {
    super(path, ptype, getBundle(ptype));
  }

  public AvroFileSource(Path path, AvroType<T> ptype, AvroMode mode) {
    super(path, ptype, getBundle(ptype, mode));
  }

  public AvroFileSource(Path path, AvroType<T> ptype, DatumReader<T> reader) {
    super(path, ptype, getBundle(ptype));
    this.reader = reader;
  }

  public AvroFileSource(List<Path> paths, AvroType<T> ptype) {
    super(paths, ptype, getBundle(ptype));
  }

  public AvroFileSource(List<Path> paths, AvroType<T> ptype, AvroMode mode) {
    super(paths, ptype, getBundle(ptype, mode));
  }
  
  public AvroFileSource(List<Path> paths, AvroType<T> ptype, DatumReader<T> reader) {
    super(paths, ptype, getBundle(ptype));
    this.reader = reader;
  }  

  @Override
  public String toString() {
    return "Avro(" + pathsAsString() + ")";
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, getFileReaderFactory((AvroType<T>) ptype));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new AvroReadableData<T>(this.paths, (AvroType<T>) ptype);
  }

  protected AvroFileReaderFactory<T> getFileReaderFactory(AvroType<T> ptype){
    return new AvroFileReaderFactory(reader, ptype);
  }
}
