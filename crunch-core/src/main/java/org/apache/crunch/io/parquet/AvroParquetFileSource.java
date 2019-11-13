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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetInputSplit;

public class AvroParquetFileSource<T extends IndexedRecord> extends FileSourceImpl<T> implements ReadableSource<T> {

  private static final String AVRO_READ_SCHEMA = "parquet.avro.read.schema";

  private final String projSchema;

  private static <S> FormatBundle<AvroParquetInputFormat> getBundle(
      AvroType<S> ptype,
      Schema projSchema,
      Class<? extends UnboundRecordFilter> filterClass) {
    FormatBundle<AvroParquetInputFormat> fb = FormatBundle.forInput(AvroParquetInputFormat.class)
        .set(AVRO_READ_SCHEMA, ptype.getSchema().toString());

    if (projSchema != null) {
      fb.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, projSchema.toString());
    }
    if (filterClass != null) {
      fb.set("parquet.read.filter", filterClass.getName());
    }
    if (!FileSplit.class.isAssignableFrom(ParquetInputSplit.class)) {
      // Older ParquetRecordReader expects ParquetInputSplits, not FileSplits, so it
      // doesn't work with CombineFileInputFormat
      fb.set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    }
    return fb;
  }

  public AvroParquetFileSource(Path path, AvroType<T> ptype) {
    this(ImmutableList.of(path), ptype);
  }

  /**
   * Read the Parquet data at the given path using the schema of the {@code AvroType}, and projecting
   * a subset of the columns from this schema via the separately given {@code Schema}.
   *
   * @param path the path of the file to read
   * @param ptype the AvroType to use in reading the file
   * @param projSchema the subset of columns from the input schema to read
   */
  public AvroParquetFileSource(Path path, AvroType<T> ptype, Schema projSchema) {
    this(ImmutableList.of(path), ptype, projSchema);
  }

  public AvroParquetFileSource(List<Path> paths, AvroType<T> ptype) {
    this(paths, ptype, null, null);
  }

  /**
   * Read the Parquet data at the given paths using the schema of the {@code AvroType}, and projecting
   * a subset of the columns from this schema via the separately given {@code Schema}.
   *
   * @param paths the list of paths to read
   * @param ptype the AvroType to use in reading the file
   * @param projSchema the subset of columns from the input schema to read
   */
  public AvroParquetFileSource(List<Path> paths, AvroType<T> ptype, Schema projSchema) {
    this(paths, ptype, projSchema, null);
  }

  public AvroParquetFileSource(List<Path> paths, AvroType<T> ptype,
                               Class<? extends UnboundRecordFilter> filterClass) {
    this(paths, ptype, null, filterClass);
  }

  /**
   * Read the Parquet data at the given paths using the schema of the {@code AvroType}, projecting
   * a subset of the columns from this schema via the separately given {@code Schema}, and using
   * the filter class to select the input records.
   *
   * @param paths the list of paths to read
   * @param ptype the AvroType to use in reading the file
   * @param projSchema the subset of columns from the input schema to read
   */
  public AvroParquetFileSource(List<Path> paths, AvroType<T> ptype, Schema projSchema,
                               Class<? extends UnboundRecordFilter> filterClass) {
    super(paths, ptype, getBundle(ptype, projSchema, filterClass));
    this.projSchema = projSchema == null ? null : projSchema.toString();
  }

  public Schema getProjectedSchema() {
    return (new Schema.Parser()).parse(projSchema);
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, getFileReaderFactory((AvroType<T>) ptype));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new AvroParquetReadableData<T>(paths, (AvroType<T>) ptype);
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
    return "Parquet(" + pathsAsString() + ((projSchema == null) ? ")" : ") -> " + projSchema);
  }

  public static <T extends SpecificRecord> Builder<T> builder(Class<T> clazz) {
    return new Builder<T>(Preconditions.checkNotNull(clazz));
  }

  public static Builder<GenericRecord> builder(Schema schema) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()));
    return new Builder(schema);
  }

  /**
   * Helper class for constructing an {@code AvroParquetFileSource} that only reads a subset of the
   * fields defined in an Avro schema.
   */
  public static class Builder<T extends IndexedRecord> {
    private Class<T> clazz;
    private Schema baseSchema;
    private List<Schema.Field> fields = Lists.newArrayList();
    private Class<? extends UnboundRecordFilter> filterClass;

    private Builder(Class<T> clazz) {
      this.clazz = clazz;
      this.baseSchema = ReflectionUtils.newInstance(clazz, null).getSchema();
    }

    private Builder(Schema baseSchema) {
      this.baseSchema = baseSchema;
    }

    public Builder includeField(String fieldName) {
      Schema.Field field = baseSchema.getField(fieldName);
      if (field == null) {
        throw new IllegalArgumentException("No field " + fieldName + " in schema: " + baseSchema.getName());
      }
      fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
      return this;
    }

    public Builder filterClass(Class<? extends UnboundRecordFilter> filterClass) {
      this.filterClass = filterClass;
      return this;
    }

    public AvroParquetFileSource<T> build(Path path) {
      return build(ImmutableList.of(path));
    }

    public AvroParquetFileSource<T> build(List<Path> paths) {
      AvroType at = clazz == null ? Avros.generics(baseSchema) : Avros.specifics((Class) clazz);
      if (fields.isEmpty()) {
        return new AvroParquetFileSource<T>(paths, at, filterClass);
      } else {
        Schema projected = Schema.createRecord(
            baseSchema.getName(),
            baseSchema.getDoc(),
            baseSchema.getNamespace(),
            baseSchema.isError());
        projected.setFields(fields);
        return new AvroParquetFileSource<T>(paths, at, projected, filterClass);
      }
    }
  }
}
