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

import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.avro.AvroPathPerKeyTarget;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@link org.apache.crunch.Target} that wraps {@link AvroParquetPathPerKeyOutputFormat} to allow one file
 * per key to be written as the output of a {@code PTable<String, T>}.
 *
 * <p>Note the restrictions that apply to the {@code AvroParquetPathPerKeyOutputFormat}; in particular, it's a good
 * idea to write out all of the records for the same key together within each partition of the data.
 */
public class AvroParquetPathPerKeyTarget extends AvroPathPerKeyTarget {

  public AvroParquetPathPerKeyTarget(String path) {
    this(new Path(path));
  }

  public AvroParquetPathPerKeyTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public AvroParquetPathPerKeyTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, AvroParquetPathPerKeyOutputFormat.class, fileNamingScheme);
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType) ((PTableType) ptype).getValueType();
    String schemaParam;
    if (name == null) {
      schemaParam = AvroParquetFileTarget.PARQUET_AVRO_SCHEMA_PARAMETER;
    } else {
      schemaParam = AvroParquetFileTarget.PARQUET_AVRO_SCHEMA_PARAMETER + "." + name;
    }
    FormatBundle fb = FormatBundle.forOutput(AvroParquetPathPerKeyOutputFormat.class);
    fb.set(schemaParam, atype.getSchema().toString());
    configureForMapReduce(job, Void.class, atype.getTypeClass(), fb, outputPath, name);
  }

  @Override
  public String toString() {
    return "AvroParquetPathPerKey(" + path + ")";
  }
}
