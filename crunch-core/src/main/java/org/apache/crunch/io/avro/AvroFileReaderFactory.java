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
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.crunch.MapFn;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroFileReaderFactory.class);

  private DatumReader<T> reader;
  private final AvroType<?> atype;
  private final MapFn<T, T> mapFn;

  public AvroFileReaderFactory(Schema schema) {
    this(null, Avros.generics(schema));
  }

  public AvroFileReaderFactory(AvroType<?> atype) {
    this(null, atype);
  }

  public AvroFileReaderFactory(DatumReader<T> reader, AvroType<?> atype) {
    this.reader = reader;
    this.atype = atype;
    this.mapFn = (MapFn<T, T>) atype.getInputMapFn();
  }

  static <T> DatumReader<T> createDatumReader(AvroType<T> atype) {
    return Avros.newReader(atype);
  }

  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {
    AvroMode mode = AvroMode.fromType(atype).withFactoryFromConfiguration(fs.getConf());
    final DatumReader recordReader = reader == null ? mode.getReader(atype.getSchema()) : reader;
    this.mapFn.initialize();
    try {
      FsInput fsi = new FsInput(path, fs.getConf());
      final DataFileReader<T> reader = new DataFileReader<T>(fsi, recordReader);
      return new AutoClosingIterator<T>(reader, new UnmodifiableIterator<T>() {
        @Override
        public boolean hasNext() {
          return reader.hasNext();
        }

        @Override
        public T next() {
          return mapFn.map(reader.next());
        }
      });
    } catch (IOException e) {
      LOG.info("Could not read avro file at path: {}", path, e);
      return Iterators.emptyIterator();
    }
  }
}
