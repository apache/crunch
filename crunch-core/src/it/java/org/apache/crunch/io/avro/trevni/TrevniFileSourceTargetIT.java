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
package org.apache.crunch.io.avro.trevni;

import com.google.common.collect.Lists;
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
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("serial")
public class TrevniFileSourceTargetIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro.trevni");
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
    ColumnFileMetaData cfmd = new ColumnFileMetaData();
    AvroColumnWriter writer = new AvroColumnWriter(schema, cfmd);

    for (GenericRecord record : genericRecords) {
      writer.write(record);
    }

    writer.writeTo(avroFile);
  }

  @Test
  public void testSpecific() throws IOException {
    GenericRecord savedRecord = new Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(TrevniFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(new TrevniKeySource(new Path(avroFile.getAbsolutePath()),
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

    Pipeline pipeline = new MRPipeline(TrevniFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Record> genericCollection = pipeline.read(new TrevniKeySource(new Path(avroFile.getAbsolutePath()),
        Avros.generics(genericPersonSchema)));

    List<Record> recordList = Lists.newArrayList(genericCollection.materialize());

    assertEquals(Lists.newArrayList(savedRecord), Lists.newArrayList(recordList));
  }

  @Test
  public void testReflect() throws IOException {
    AvroType<StringWrapper> strType = Avros.reflects (StringWrapper.class);
    Schema schema = strType.getSchema();
    GenericRecord savedRecord = new Record(schema);
    savedRecord.put("value", "stringvalue");
    populateGenericFile(Lists.newArrayList(savedRecord), schema);

    Pipeline pipeline = new MRPipeline(TrevniFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<StringWrapper> stringValueCollection = pipeline.read(new TrevniKeySource(new Path(avroFile.getAbsolutePath()),
        strType));

    List<StringWrapper> recordList = Lists.newArrayList(stringValueCollection.materialize());

    assertEquals(1, recordList.size());
    StringWrapper stringWrapper = recordList.get(0);
    assertEquals("stringvalue", stringWrapper.getValue());
  }
}
