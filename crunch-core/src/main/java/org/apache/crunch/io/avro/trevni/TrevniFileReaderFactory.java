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
package org.apache.crunch.io.avro.trevni;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.Input;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.HadoopInput;

import java.io.IOException;
import java.util.Iterator;

public class TrevniFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(TrevniFileReaderFactory.class);
  private final AvroType<T> aType;
  private final MapFn<T, T> mapFn;
  private final Schema schema;

  public TrevniFileReaderFactory(AvroType<T> atype) {
    this.aType = atype;
    schema = atype.getSchema();
    this.mapFn = (MapFn<T, T>) atype.getInputMapFn();
  }

  public TrevniFileReaderFactory(Schema schema) {
    this.aType = null;
    this.schema = schema;
    this.mapFn = IdentityFn.<T>getInstance();
  }

  static <T> AvroColumnReader<T> getReader(Input input, AvroType<T> avroType, Schema schema) {
    AvroColumnReader.Params params = new AvroColumnReader.Params(input);
    params.setSchema(schema);
    if (avroType.hasReflect()) {
      if (avroType.hasSpecific()) {
        Avros.checkCombiningSpecificAndReflectionSchemas();
      }
      params.setModel(ReflectData.get());
    } else if (avroType.hasSpecific()) {
      params.setModel(SpecificData.get());
    } else {
      params.setModel(GenericData.get());
    }

    try {
      return new AvroColumnReader<T>(params);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {
    this.mapFn.initialize();
    try {
      HadoopInput input = new HadoopInput(path, fs.getConf());
      final AvroColumnReader<T> reader = getReader(input, aType, schema);
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
      LOG.info("Could not read avro file at path: " + path, e);
      return Iterators.emptyIterator();
    }
  }
}
