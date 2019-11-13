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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.parquet.avro.AvroParquetWriter;

import com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

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
  
  @Test
  public void testProjectionSpecific() throws IOException {
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(
        AvroParquetFileSource.builder(Person.class)
            .includeField("age")
            .build(new Path(avroFile.getAbsolutePath())));

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.write(avroFile);
    pipeline.done();
    
    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<Person> ageOnly = pipeline2.read(
        new AvroFileSource<Person>(new Path(outputFile.getAbsolutePath()), Avros.specifics(Person.class)));

    Person person = Iterables.getOnlyElement(ageOnly.materialize());
    assertNull(person.getName());
    assertEquals(person.getAge(), 42);
    assertNull(person.getSiblingnames());
  }

  @Test
  public void testProjectionGeneric() throws IOException {
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    AvroParquetFileSource<GenericRecord> src = AvroParquetFileSource.builder(Person.SCHEMA$)
        .includeField("age")
        .build(new Path(avroFile.getAbsolutePath()));
    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<GenericRecord> genericCollection = pipeline.read(src);

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.write(avroFile);
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<Record> ageOnly = pipeline2.read(
        new AvroFileSource<Record>(new Path(outputFile.getAbsolutePath()), Avros.generics(src.getProjectedSchema())));

    Record person = Iterables.getOnlyElement(ageOnly.materialize());
    assertEquals(person.get(0), 42);
    try {
      person.get(1);
      fail("Trying to get field outside of projection should fail");
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }
  }

  @Test
  public void testCustomReadSchema_FieldSubset() throws IOException {
    Schema readSchema = SchemaBuilder.record("PersonSubset")
        .namespace("org.apache.crunch.test")
        .fields()
        .optionalString("name")
        .endRecord();
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<GenericRecord> genericCollection = pipeline.read(
        AvroParquetFileSource.builder(readSchema)
            .includeField("name")
            .build(new Path(avroFile.getAbsolutePath())));

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.write(avroFile);
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<GenericData.Record> namedPersonRecords = pipeline2.read(
        From.avroFile(new Path(outputFile.getAbsolutePath())));

    GenericRecord personSubset = Iterables.getOnlyElement(namedPersonRecords.materialize());

    assertEquals(readSchema, personSubset.getSchema());
    assertEquals(new Utf8("John Doe"), personSubset.get("name"));
  }

  @Test
  public void testCustomReadSchemaGeneric_FieldSuperset() throws IOException {
    Schema readSchema = SchemaBuilder.record("PersonSuperset")
        .namespace("org.apache.crunch.test")
        .fields()
        .optionalString("name")
        .optionalInt("age")
        .name("siblingnames").type(Person.SCHEMA$.getField("siblingnames").schema()).withDefault(null)
        .name("employer").type().stringType().stringDefault("Acme Corp")
        .endRecord();
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<GenericRecord> genericCollection = pipeline.read(
        AvroParquetFileSource.builder(readSchema)
            .build(new Path(avroFile.getAbsolutePath())));

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.write(avroFile);
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<GenericData.Record> namedPersonRecords = pipeline2.read(
        From.avroFile(new Path(outputFile.getAbsolutePath())));

    GenericRecord personSuperset = Iterables.getOnlyElement(namedPersonRecords.materialize());

    assertEquals(readSchema, personSuperset.getSchema());
    assertEquals(new Utf8("John Doe"), personSuperset.get("name"));
    assertEquals(42, personSuperset.get("age"));
    assertEquals(Lists.newArrayList(new Utf8("Jimmy"), new Utf8("Jane")), personSuperset.get("siblingnames"));
    assertEquals(new Utf8("Acme Corp"), personSuperset.get("employer"));
  }

  @Test
  public void testCustomReadSchemaWithProjection() throws IOException {
    Schema readSchema = SchemaBuilder.record("PersonSubsetWithProjection")
        .namespace("org.apache.crunch.test")
        .fields()
        .optionalString("name")
        .optionalInt("age")
        .endRecord();
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<GenericRecord> genericCollection = pipeline.read(
        AvroParquetFileSource.builder(readSchema)
            .includeField("name")
            .build(new Path(avroFile.getAbsolutePath())));

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.write(avroFile);
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<GenericData.Record> namedPersonRecords = pipeline2.read(
        From.avroFile(new Path(outputFile.getAbsolutePath())));

    GenericRecord personSubset = Iterables.getOnlyElement(namedPersonRecords.materialize());

    assertEquals(readSchema, personSubset.getSchema());
    assertEquals(new Utf8("John Doe"), personSubset.get("name"));
    assertNull(personSubset.get("age"));
  }

  @Test
  public void testProjectionFiltered() throws IOException {
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(
        AvroParquetFileSource.builder(Person.class)
            .includeField("age")
            .filterClass(RejectAllFilter.class)
            .build(new Path(avroFile.getAbsolutePath())));

    File outputFile = tmpDir.getFile("output");
    Target avroFile = To.avroFile(outputFile.getAbsolutePath());
    genericCollection.filter(new FilterFn<Person>() {
      @Override
      public boolean accept(Person input) {
        return input != null;
      }
    }).write(avroFile);
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(AvroParquetFileSourceTargetIT.class,
        tmpDir.getDefaultConfiguration());
    PCollection<Person> ageOnly = pipeline2.read(
        new AvroFileSource<Person>(new Path(outputFile.getAbsolutePath()), Avros.specifics(Person.class)));
    assertTrue(Lists.newArrayList(ageOnly.materialize()).isEmpty());
  }

  public static class RejectAllFilter implements UnboundRecordFilter {
    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return new RecordFilter() {
        @Override
        public boolean isMatch() {
          return false;
        }
      };
    }
  }
}
