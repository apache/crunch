/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroMemPipelineIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro");
  }

  @Test
  public void testMemPipelienWithSpecificRecord() {

    Person writeRecord = createSpecificRecord();

    final PCollection<Person> writeCollection = MemPipeline.collectionOf(Collections.singleton(writeRecord));

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<Person> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(), Avros.records(Person.class)));

    Person readRecord = readCollection.materialize().iterator().next();

    assertEquals(writeRecord, readRecord);
  }

  private Person createSpecificRecord() {
    List<CharSequence> siblingnames = Lists.newArrayList();
    return new Person("John", 41, siblingnames);
  }

  @Test
  public void testMemPipelienWithGenericRecord() {

    GenericRecord writeRecord = createGenericRecord();

    final PCollection<GenericRecord> writeCollection = MemPipeline.collectionOf(Collections.singleton(writeRecord));

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<Record> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(), Avros.generics(writeRecord.getSchema())));

    Record readRecord = readCollection.materialize().iterator().next();

    assertEquals(writeRecord, readRecord);
  }

  private GenericRecord createGenericRecord() {

    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));

    return savedRecord;
  }

  @Test
  public void testMemPipelienWithReflectionRecord() {

    String writeRecord = "John Doe";

    final PCollection<String> writeCollection = MemPipeline.collectionOf(Collections.singleton(writeRecord));

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<? extends String> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(), Avros.reflects(writeRecord.getClass())));

    Object readRecord = readCollection.materialize().iterator().next();

    assertEquals(writeRecord, readRecord.toString());
  }

  @Test
  public void testMemPipelineWithMultiplePaths() {

    GenericRecord writeRecord1 = createGenericRecord("John Doe");
    final PCollection<GenericRecord> writeCollection1 = MemPipeline.collectionOf(Collections.singleton(writeRecord1));
    writeCollection1.write(To.avroFile(avroFile.getAbsolutePath()));

    File avroFile2 = tmpDir.getFile("test2.avro");
    GenericRecord writeRecord2 = createGenericRecord("Jane Doe");
    final PCollection<GenericRecord> writeCollection2 = MemPipeline.collectionOf(Collections.singleton(writeRecord2));
    writeCollection2.write(To.avroFile(avroFile2.getAbsolutePath()));

    List<Path> paths = Lists.newArrayList(new Path(avroFile.getAbsolutePath()),
        new Path(avroFile2.getAbsolutePath()));
    PCollection<Record> readCollection = MemPipeline.getInstance().read(
        new AvroFileSource<Record>(paths, Avros.generics(writeRecord1.getSchema())));

    Set<Record> readSet = Sets.newHashSet(readCollection.materialize());

    assertEquals(Sets.newHashSet(writeRecord1, writeRecord2), readSet);
  }

  private GenericRecord createGenericRecord(String name) {

    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", name);
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy"));

    return savedRecord;
  }

}
