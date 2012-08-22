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
package org.apache.crunch.io.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.crunch.Pair;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroFileReaderFactoryTest {

  private File avroFile;

  @Before
  public void setUp() throws IOException {
    avroFile = File.createTempFile("test", ".av");
  }

  @After
  public void tearDown() {
    avroFile.delete();
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema outputSchema) throws IOException {
    FileOutputStream outputStream = new FileOutputStream(this.avroFile);
    GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(outputSchema);

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(genericDatumWriter);
    dataFileWriter.create(outputSchema, outputStream);

    for (GenericRecord record : genericRecords) {
      dataFileWriter.append(record);
    }

    dataFileWriter.close();
    outputStream.close();

  }

  private <T> AvroFileReaderFactory<T> createFileReaderFactory(AvroType<T> avroType) {
    return new AvroFileReaderFactory<T>(avroType, new Configuration());
  }

  @Test
  public void testRead_GenericReader() throws IOException {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    AvroFileReaderFactory<GenericData.Record> genericReader = createFileReaderFactory(Avros.generics(Person.SCHEMA$));
    Iterator<GenericData.Record> recordIterator = genericReader.read(FileSystem.getLocal(new Configuration()),
        new Path(this.avroFile.getAbsolutePath()));

    GenericRecord genericRecord = recordIterator.next();
    assertEquals(savedRecord, genericRecord);
    assertFalse(recordIterator.hasNext());
  }

  @Test
  public void testRead_SpecificReader() throws IOException {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    AvroFileReaderFactory<Person> genericReader = createFileReaderFactory(Avros.records(Person.class));
    Iterator<Person> recordIterator = genericReader.read(FileSystem.getLocal(new Configuration()), new Path(
        this.avroFile.getAbsolutePath()));

    Person expectedPerson = new Person();
    expectedPerson.age = 42;
    expectedPerson.name = "John Doe";
    List<CharSequence> siblingNames = Lists.newArrayList();
    siblingNames.add("Jimmy");
    siblingNames.add("Jane");
    expectedPerson.siblingnames = siblingNames;

    Person person = recordIterator.next();

    assertEquals(expectedPerson, person);
    assertFalse(recordIterator.hasNext());
  }

  @Test
  public void testRead_ReflectReader() throws IOException {
    Schema reflectSchema = ReflectData.get().getSchema(StringWrapper.class);
    GenericRecord savedRecord = new GenericData.Record(reflectSchema);
    savedRecord.put("value", "stringvalue");
    populateGenericFile(Lists.newArrayList(savedRecord), reflectSchema);

    AvroFileReaderFactory<StringWrapper> genericReader = createFileReaderFactory(Avros.reflects(StringWrapper.class));
    Iterator<StringWrapper> recordIterator = genericReader.read(FileSystem.getLocal(new Configuration()), new Path(
        this.avroFile.getAbsolutePath()));

    StringWrapper stringWrapper = recordIterator.next();

    assertEquals("stringvalue", stringWrapper.getValue());
    assertFalse(recordIterator.hasNext());
  }

  @Test
  public void testCreateDatumReader_Generic() {
    DatumReader<Record> datumReader = AvroFileReaderFactory.createDatumReader(Avros.generics(Person.SCHEMA$));
    assertEquals(GenericDatumReader.class, datumReader.getClass());
  }

  @Test
  public void testCreateDatumReader_Reflect() {
    DatumReader<StringWrapper> datumReader = AvroFileReaderFactory.createDatumReader(Avros
        .reflects(StringWrapper.class));
    assertEquals(ReflectDatumReader.class, datumReader.getClass());
  }

  @Test
  public void testCreateDatumReader_Specific() {
    DatumReader<Person> datumReader = AvroFileReaderFactory.createDatumReader(Avros.records(Person.class));
    assertEquals(SpecificDatumReader.class, datumReader.getClass());
  }

  @Test
  public void testCreateDatumReader_ReflectAndSpecific() {
    Assume.assumeTrue(Avros.CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS);

    DatumReader<Pair<Person, StringWrapper>> datumReader = AvroFileReaderFactory.createDatumReader(Avros.pairs(
        Avros.records(Person.class), Avros.reflects(StringWrapper.class)));
    assertEquals(ReflectDatumReader.class, datumReader.getClass());
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateDatumReader_ReflectAndSpecific_NotSupported() {
    Assume.assumeTrue(!Avros.CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS);
    AvroFileReaderFactory.createDatumReader(Avros.pairs(Avros.records(Person.class),
        Avros.reflects(StringWrapper.class)));
  }

}
