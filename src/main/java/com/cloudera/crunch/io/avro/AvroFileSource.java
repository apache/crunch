/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.io.CompositePathIterable;
import com.cloudera.crunch.io.ReadableSource;
import com.cloudera.crunch.io.impl.FileSourceImpl;
import com.cloudera.crunch.io.impl.InputBundle;
import com.cloudera.crunch.types.avro.AvroInputFormat;
import com.cloudera.crunch.types.avro.AvroType;
import com.cloudera.crunch.types.avro.Avros;

public class AvroFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  public AvroFileSource(Path path, AvroType<T> ptype) {
    super(path, ptype, new InputBundle(AvroInputFormat.class)
        .set(AvroJob.INPUT_IS_REFLECT, String.valueOf(!ptype.isSpecific()))
        .set(AvroJob.INPUT_SCHEMA, ptype.getSchema().toString())
        .set(Avros.REFLECT_DATA_FACTORY_CLASS, Avros.REFLECT_DATA_FACTORY.getClass().getName()));
  }

  @Override
  public String toString() {
    return "Avro(" + path.toString() + ")";
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return CompositePathIterable.create(FileSystem.get(conf), path, new AvroFileReaderFactory<T>(
        (AvroType<T>) ptype, conf));
  }
}
