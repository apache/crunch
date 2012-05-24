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
package com.cloudera.crunch.io.avro;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.io.FileReaderFactory;
import com.cloudera.crunch.types.avro.AvroType;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

public class AvroFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(AvroFileReaderFactory.class);

  private final DatumReader<T> recordReader;
  private final MapFn<T, T> mapFn;
  private final Configuration conf;
  
  public AvroFileReaderFactory(AvroType<T> atype, Configuration conf) {
    this.recordReader = createDatumReader(atype);
    this.mapFn = (MapFn<T, T>) atype.getInputMapFn();
    this.conf = conf;
  }

  private DatumReader<T> createDatumReader(AvroType<T> avroType) {
    if (avroType.isSpecific()) {
      return new SpecificDatumReader<T>(avroType.getSchema());
    } else {
      return new GenericDatumReader<T>(avroType.getSchema());
    }
  }

  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {
    this.mapFn.setConfigurationForTest(conf);
    this.mapFn.initialize();
    try {
      FsInput fsi = new FsInput(path, fs.getConf());
      final DataFileReader<T> reader = new DataFileReader<T>(fsi, recordReader);
      return new UnmodifiableIterator<T>() {
        @Override
        public boolean hasNext() {
          return reader.hasNext();
        }

        @Override
        public T next() {
          return mapFn.map(reader.next());
        }
      };
    } catch (IOException e) {
      LOG.info("Could not read avro file at path: " + path, e);
      return Iterators.emptyIterator();
    }
  }
}
