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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test {@link org.apache.crunch.types.avro.SafeAvroSerialization} with Specific Avro types
 */
public class SpecificAvroGroupByIT implements Serializable {

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
  public void testGrouByWithSpecificAvroType() throws Exception {
    MRPipeline pipeline = new MRPipeline(SpecificAvroGroupByIT.class, tmpDir.getDefaultConfiguration());
    testSpecificAvro(pipeline);
  }

  public void testSpecificAvro(MRPipeline pipeline) throws Exception {

    createPersonAvroFile(avroFile);

    PCollection<Person> unsorted = pipeline.read(At.avroFile(avroFile.getAbsolutePath(), Avros.records(Person.class)));

    PTable<String, Person> sorted = unsorted.parallelDo(new MapFn<Person, Pair<String, Person>>() {

      @Override
      public Pair<String, Person> map(Person input) {
        String key = input.name.toString();
        return Pair.of(key, input);

      }
    }, Avros.tableOf(Avros.strings(), Avros.records(Person.class))).groupByKey().ungroup();

    List<Pair<String, Person>> outputPersonList = Lists.newArrayList(sorted.materialize());

    assertEquals(1, outputPersonList.size());
    assertEquals(String.class, outputPersonList.get(0).first().getClass());
    assertEquals(Person.class, outputPersonList.get(0).second().getClass());

    pipeline.done();
  }

  private static void createPersonAvroFile(File avroFile) throws IOException {

    Person person = new Person();
    person.age = 40;
    person.name = "Bob";
    List<CharSequence> siblingNames = Lists.newArrayList();
    siblingNames.add("Bob1");
    siblingNames.add("Bob2");
    person.siblingnames = siblingNames;

    FileOutputStream outputStream = new FileOutputStream(avroFile);
    SpecificDatumWriter<Person> writer = new SpecificDatumWriter<Person>(Person.class);

    DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(writer);
    dataFileWriter.create(Person.SCHEMA$, outputStream);
    dataFileWriter.append(person);
    dataFileWriter.close();
    outputStream.close();
  }
}
