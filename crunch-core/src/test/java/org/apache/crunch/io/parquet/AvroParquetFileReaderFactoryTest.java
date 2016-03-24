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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.apache.parquet.avro.AvroParquetWriter;

public class AvroParquetFileReaderFactoryTest {

  private File parquetFile;

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    parquetFile = tmpDir.getFile("test.avro.parquet");
  }

  @After
  public void tearDown() {
    parquetFile.delete();
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        new Path(parquetFile.getPath()), schema);

    for (GenericRecord record : genericRecords) {
      writer.write(record);
    }

    writer.close();
  }

  private <T> AvroParquetFileReaderFactory<T> createFileReaderFactory(AvroType<T> avroType) {
    return new AvroParquetFileReaderFactory<T>(avroType);
  }

  @Test
  public void testProjection() throws IOException {
    String genericSchemaJson = Person.SCHEMA$.toString().replace("Person", "GenericPerson");
    Schema genericPersonSchema = new Schema.Parser().parse(genericSchemaJson);
    GenericRecord savedRecord = new Record(genericPersonSchema);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), genericPersonSchema);

    Schema projection = Schema.createRecord("projection", null, null, false);
    projection.setFields(Lists.newArrayList(cloneField(genericPersonSchema.getField("name"))));
    AvroParquetFileReaderFactory<Record> genericReader = createFileReaderFactory(Avros.generics(projection));
    Iterator<Record> recordIterator = genericReader.read(FileSystem.getLocal(new Configuration()),
        new Path(this.parquetFile.getAbsolutePath()));

    GenericRecord genericRecord = recordIterator.next();
    assertEquals(savedRecord.get("name"), genericRecord.get("name"));
    assertNull(genericRecord.get("age"));
    assertFalse(recordIterator.hasNext());
  }

  public static Schema.Field cloneField(Schema.Field field) {
    return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
  }

}
