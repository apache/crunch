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
package org.apache.crunch.io.parquet;

import java.io.IOException;
import java.util.List;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroReadSupport;

public class AvroParquetFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  private static <S> FormatBundle<AvroParquetInputFormat> getBundle(AvroType<S> ptype) {
    return FormatBundle.forInput(AvroParquetInputFormat.class)
        .set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, ptype.getSchema().toString())
        // ParquetRecordReader expects ParquetInputSplits, not FileSplits, so it
        // doesn't work with CombineFileInputFormat
        .set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
  }

  public AvroParquetFileSource(Path path, AvroType<T> ptype) {
    super(path, ptype, getBundle(ptype));
  }

  public AvroParquetFileSource(List<Path> paths, AvroType<T> ptype) {
    super(paths, ptype, getBundle(ptype));
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, getFileReaderFactory((AvroType<T>) ptype));
  }

  protected AvroParquetFileReaderFactory<T> getFileReaderFactory(AvroType<T> ptype){
    return new AvroParquetFileReaderFactory<T>(ptype);
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return new AvroParquetConverter<T>((AvroType<T>) ptype);
  }

  @Override
  public String toString() {
    return "Parquet(" + pathsAsString() + ")";
  }
}