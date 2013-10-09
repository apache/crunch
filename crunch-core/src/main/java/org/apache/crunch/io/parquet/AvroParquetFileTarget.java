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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;

public class AvroParquetFileTarget extends FileTargetImpl {

  private static final String PARQUET_AVRO_SCHEMA_PARAMETER = "parquet.avro.schema";

  public AvroParquetFileTarget(String path) {
    this(new Path(path));
  }

  public AvroParquetFileTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public AvroParquetFileTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, CrunchAvroParquetOutputFormat.class, fileNamingScheme);
  }

  @Override
  public String toString() {
    return "Parquet(" + path.toString() + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    return new AvroParquetConverter<Object>((AvroType<Object>) ptype);
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType<?>) ptype;
    Configuration conf = job.getConfiguration();
    String schemaParam;
    if (name == null) {
      schemaParam = PARQUET_AVRO_SCHEMA_PARAMETER;
    } else {
      schemaParam = PARQUET_AVRO_SCHEMA_PARAMETER + "." + name;
    }
    String outputSchema = conf.get(schemaParam);
    if (outputSchema == null) {
      conf.set(schemaParam, atype.getSchema().toString());
    } else if (!outputSchema.equals(atype.getSchema().toString())) {
      throw new IllegalStateException("Avro targets must use the same output schema");
    }
    configureForMapReduce(job, Void.class, atype.getTypeClass(),
        CrunchAvroParquetOutputFormat.class, outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof AvroType) {
      return new AvroParquetFileSourceTarget<T>(path, (AvroType<T>) ptype);
    }
    return null;
  }

  static class CrunchAvroWriteSupport extends AvroWriteSupport {
    @Override
    public WriteContext init(Configuration conf) {
      String outputName = conf.get("crunch.namedoutput");
      if (outputName != null && !outputName.isEmpty()) {
        String schema = conf.get(PARQUET_AVRO_SCHEMA_PARAMETER + "." + outputName);
        setSchema(conf, new Schema.Parser().parse(schema));
      }
      return super.init(conf);
    }
  }

  static class CrunchAvroParquetOutputFormat extends ParquetOutputFormat<IndexedRecord> {

    public CrunchAvroParquetOutputFormat() {
      super(new CrunchAvroWriteSupport());
    }
  }

}
