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

import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetReader;
import parquet.schema.MessageType;

class AvroParquetFileReaderFactory<T> implements FileReaderFactory<T> {

  private AvroType<T> avroType;

  public AvroParquetFileReaderFactory(AvroType<T> avroType) {
    this.avroType = avroType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<T> read(FileSystem fs, Path path) {
    Path p = fs.makeQualified(path);
    final ParquetReader reader;
    try {
      reader = new ParquetReader(p, new CrunchAvroReadSupport(avroType));
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
    return new AutoClosingIterator<T>(reader, new UnmodifiableIterator<T>() {

      private T next;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        try {
          next = (T) reader.read();
        } catch (IOException e) {
          throw new CrunchRuntimeException(e);
        }
        return next != null;
      }

      @Override
      public T next() {
        if (hasNext()) {
          T ret = next;
          next = null;
          return ret;
        }
        throw new NoSuchElementException();
      }
    });

  }

  static class CrunchAvroReadSupport<T extends IndexedRecord> extends AvroReadSupport<T> {
    private AvroType<T> avroType;

    public CrunchAvroReadSupport(AvroType<T> avroType) {
      this.avroType = avroType;
    }

    @Override
    public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
      if (avroType != null) {
        setRequestedProjection(configuration, avroType.getSchema());
      }
      return super.init(configuration, keyValueMetaData, fileSchema);
    }
  }
}
