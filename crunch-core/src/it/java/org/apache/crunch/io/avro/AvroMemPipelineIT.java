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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class AvroMemPipelineIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro");
  }

  @Test
  public void testMemPipelineWithSpecificRecord() {

    Person writeRecord = createSpecificRecord();

    final PCollection<Person> writeCollection = MemPipeline.getInstance().create(
            ImmutableList.of(writeRecord), Avros.specifics(Person.class));

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
  public void testMemPipelineWithGenericRecord() {

    PType<GenericData.Record> ptype = Avros.generics(Person.SCHEMA$);

    GenericData.Record writeRecord = createGenericRecord("John Doe");

    final PCollection<GenericData.Record> writeCollection = MemPipeline.typedCollectionOf(
                                                            ptype,
                                                            writeRecord);

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<Record> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(), Avros.generics(writeRecord.getSchema())));

    Record readRecord = readCollection.materialize().iterator().next();

    assertEquals(writeRecord, readRecord);
  }

  @Test
  public void testMemPipelineWithReflectionRecord() {

    String writeRecord = "John Doe";

    final PCollection<String> writeCollection = MemPipeline.typedCollectionOf(
                                                          Avros.strings(),
                                                          writeRecord);

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<? extends String> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(), Avros.reflects(writeRecord.getClass())));

    Object readRecord = readCollection.materialize().iterator().next();

    assertEquals(writeRecord, readRecord.toString());
  }

  @Test
  public void testMemPipelineWithMultiplePaths() {

    PType<GenericData.Record> ptype = Avros.generics(Person.SCHEMA$);
    GenericData.Record writeRecord1 = createGenericRecord("John Doe");
    final PCollection<GenericData.Record> writeCollection1 = MemPipeline.typedCollectionOf(
      ptype,
                                                                      writeRecord1);
    writeCollection1.write(To.avroFile(avroFile.getAbsolutePath()));

    File avroFile2 = tmpDir.getFile("test2.avro");
    GenericData.Record writeRecord2 = createGenericRecord("Jane Doe");
    final PCollection<GenericData.Record> writeCollection2 = MemPipeline.typedCollectionOf(
                                                                    ptype,
                                                                    writeRecord2);
    writeCollection2.write(To.avroFile(avroFile2.getAbsolutePath()));

    List<Path> paths = Lists.newArrayList(new Path(avroFile.getAbsolutePath()),
        new Path(avroFile2.getAbsolutePath()));
    PCollection<Record> readCollection = MemPipeline.getInstance().read(
        new AvroFileSource<Record>(paths, Avros.generics(writeRecord1.getSchema())));

    Set<Record> readSet = Sets.newHashSet(readCollection.materialize());

    assertEquals(Sets.newHashSet(writeRecord1, writeRecord2), readSet);
  }

  private GenericData.Record createGenericRecord(String name) {

    GenericData.Record savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", name);
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy"));

    return savedRecord;
  }

  @Test
  public void testMemPipelineWithPTable() {

    String writeRecord = "John Doe";

    final PCollection<String> collection = MemPipeline.typedCollectionOf(
        Avros.strings(),
        writeRecord);

    PTable<Integer, String> writeCollection = collection.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, Avros.ints());

    writeCollection.write(To.avroFile(avroFile.getAbsolutePath()));

    PCollection<Pair<Integer, String>> readCollection = MemPipeline.getInstance().read(
        At.avroFile(avroFile.getAbsolutePath(),
            Avros.tableOf(Avros.ints(), Avros.strings())));

    Map<Integer, String> map = PTables.asPTable(readCollection).asMap().getValue();
    assertEquals(writeRecord, map.get(writeRecord.length()));
  }

}
