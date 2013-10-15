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
package org.apache.crunch.lib;

import static org.junit.Assert.assertEquals;
import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.records;
import static org.apache.crunch.types.avro.Avros.strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test sorting Avro types by selected inner field
 */
public class AvroTypeSortIT implements Serializable {

  private static final long serialVersionUID = 1344118240353796561L;

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = File.createTempFile("avrotest", ".avro");
  }

  @After
  public void tearDown() {
    avroFile.delete();
  }

  @Test
  public void testSortAvroTypesBySelectedFields() throws Exception {

    MRPipeline pipeline = new MRPipeline(AvroTypeSortIT.class, tmpDir.getDefaultConfiguration());

    Person ccc10 = createPerson("CCC", 10);
    Person bbb20 = createPerson("BBB", 20);
    Person aaa30 = createPerson("AAA", 30);

    writeAvroFile(Lists.newArrayList(ccc10, bbb20, aaa30), avroFile);

    PCollection<Person> unsorted = pipeline.read(At.avroFile(avroFile.getAbsolutePath(), records(Person.class)));

    // Sort by Name
    MapFn<Person, String> nameExtractor = new MapFn<Person, String>() {

      @Override
      public String map(Person input) {
        return input.name.toString();
      }
    };

    PCollection<Person> sortedByName = unsorted.by(nameExtractor, strings()).groupByKey().ungroup().values();

    List<Person> sortedByNameList = Lists.newArrayList(sortedByName.materialize());

    assertEquals(3, sortedByNameList.size());
    assertEquals(aaa30, sortedByNameList.get(0));
    assertEquals(bbb20, sortedByNameList.get(1));
    assertEquals(ccc10, sortedByNameList.get(2));

    // Sort by Age

    MapFn<Person, Integer> ageExtractor = new MapFn<Person, Integer>() {

      @Override
      public Integer map(Person input) {
        return input.age;
      }
    };

    PCollection<Person> sortedByAge = unsorted.by(ageExtractor, ints()).groupByKey().ungroup().values();

    List<Person> sortedByAgeList = Lists.newArrayList(sortedByAge.materialize());

    assertEquals(3, sortedByAgeList.size());
    assertEquals(ccc10, sortedByAgeList.get(0));
    assertEquals(bbb20, sortedByAgeList.get(1));
    assertEquals(aaa30, sortedByAgeList.get(2));

    pipeline.done();
  }

  private static void writeAvroFile(List<Person> people, File avroFile) throws IOException {

    FileOutputStream outputStream = new FileOutputStream(avroFile);
    SpecificDatumWriter<Person> writer = new SpecificDatumWriter<Person>(Person.class);

    DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(writer);
    dataFileWriter.create(Person.SCHEMA$, outputStream);
    for (Person person : people) {
      dataFileWriter.append(person);
    }
    dataFileWriter.close();
    outputStream.close();
  }

  private static Person createPerson(String name, int age) {

    Person person = new Person();
    person.age = age;
    person.name = name;
    person.siblingnames = Lists.newArrayList();

    return person;
  }
}
