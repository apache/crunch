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
package org.apache.crunch.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.Source;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FromTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected=IllegalArgumentException.class)
  public void testAvroFile_EmptyPathListNotAllowed() {
    From.avroFile(ImmutableList.<Path>of());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testTextFile_EmptyPathListNotAllowed() {
    From.textFile(ImmutableList.<Path>of());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testFormattedFile_EmptyPathListNotAllowed() {
    From.formattedFile(ImmutableList.<Path>of(), TextInputFormat.class, LongWritable.class, Text.class);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testSequenceFile_EmptyPathListNotAllowed() {
    From.sequenceFile(ImmutableList.<Path>of(), LongWritable.class, Text.class);
  }

  @Test
  public void testAvroFile_GlobWithSchemaInferenceIsSupported() throws IOException {
    Schema schema = SchemaBuilder.record("record")
            .fields()
            .endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, tmp.newFile("1"));
      writer.append(new GenericData.Record(schema));
    }

    Source<GenericData.Record> source = From.avroFile(new Path(tmp.getRoot().toString() + "/*"));

    assertEquals(source.getType(), Avros.generics(schema));
  }

  @Test
  public void testAvroFile_DirectoryWithSchemaInferenceIsSupported() throws IOException {
    Schema schema = SchemaBuilder.record("record")
            .fields()
            .endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, tmp.newFile("1"));
      writer.append(new GenericData.Record(schema));
    }

    Source<GenericData.Record> source = From.avroFile(new Path(tmp.getRoot().toString()));

    assertEquals(source.getType(), Avros.generics(schema));
  }

  @Test
  public void testAvroFile_FileWithSchemaInferenceIsSupported() throws IOException {
    Schema schema = SchemaBuilder.record("record")
            .fields()
            .endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, tmp.newFile("1"));
      writer.append(new GenericData.Record(schema));
    }

    Source<GenericData.Record> source = From.avroFile(new Path(tmp.getRoot().toString(), "1"));

    assertEquals(source.getType(), Avros.generics(schema));
  }
}