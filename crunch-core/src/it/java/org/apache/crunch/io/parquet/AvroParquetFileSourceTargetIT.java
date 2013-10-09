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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("serial")
public class AvroParquetFileSourceTargetIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro.parquet");
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        new Path(avroFile.getPath()), schema);

    for (GenericRecord record : genericRecords) {
      writer.write(record);
    }

    writer.close();
  }

  @Test
  public void testSpecific() throws IOException {
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(new AvroParquetFileSource<Person>(new Path(avroFile.getAbsolutePath()),
        Avros.records(Person.class)));

    List<Person> personList = Lists.newArrayList(genericCollection.materialize());

    Person expectedPerson = new Person();
    expectedPerson.name = "John Doe";
    expectedPerson.age = 42;

    List<CharSequence> siblingNames = Lists.newArrayList();
    siblingNames.add("Jimmy");
    siblingNames.add("Jane");
    expectedPerson.siblingnames = siblingNames;

    assertEquals(Lists.newArrayList(expectedPerson), Lists.newArrayList(personList));
  }

  @Test
  public void testGeneric() throws IOException {
    String genericSchemaJson = Person.SCHEMA$.toString().replace("Person", "GenericPerson");
    Schema genericPersonSchema = new Schema.Parser().parse(genericSchemaJson);
    GenericRecord savedRecord = new Record(genericPersonSchema);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), genericPersonSchema);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Record> genericCollection = pipeline.read(new AvroParquetFileSource<Record>(new Path
        (avroFile.getAbsolutePath()),
        Avros.generics(genericPersonSchema)));

    List<Record> recordList = Lists.newArrayList(genericCollection.materialize());

    assertEquals(Lists.newArrayList(savedRecord), Lists.newArrayList(recordList));
  }

}
