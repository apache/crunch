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
package org.apache.crunch.io.avro.trevni;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.avro.AvroColumnReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TrevniKeyPipelineIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro.trevni");
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
    FileOutputStream outputStream = new FileOutputStream(this.avroFile);
    GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(schema);

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(genericDatumWriter);
    dataFileWriter.create(schema, outputStream);

    for (GenericRecord record : genericRecords) {
      dataFileWriter.append(record);
    }

    dataFileWriter.close();
    outputStream.close();
  }

  @Test
  public void toAvroTrevniKeyTarget() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(TrevniKeyPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target trevniFile = new TrevniKeyTarget(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, trevniFile);
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    File trvFile = new File(outputFile, "part-m-00000.trv-part-0.trv");

    AvroColumnReader.Params params = new AvroColumnReader.Params(trvFile);
    params.setSchema(Person.SCHEMA$);
    params.setModel(SpecificData.get());
    AvroColumnReader<Person> reader = new AvroColumnReader<Person>(params);

    try{
      Person readPerson = reader.next();
      assertThat(readPerson, is(person));
    }finally{
      reader.close();
    }
  }

  @Test
  public void toAvroTrevniKeyMultipleTarget() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(TrevniKeyPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File output1File = tmpDir.getFile("output1");
    File output2File = tmpDir.getFile("output2");
    pipeline.write(genericCollection, new TrevniKeyTarget(output1File.getAbsolutePath()));
    pipeline.write(genericCollection, new TrevniKeyTarget(output2File.getAbsolutePath()));
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    File trv1File = new File(output1File, "part-m-00000.trv-part-0.trv");
    File trv2File = new File(output2File, "part-m-00000.trv-part-0.trv");

    AvroColumnReader.Params params = new AvroColumnReader.Params(trv1File);
    params.setSchema(Person.SCHEMA$);
    params.setModel(SpecificData.get());
    AvroColumnReader<Person> reader = new AvroColumnReader<Person>(params);

    try{
      Person readPerson = reader.next();
      assertThat(readPerson, is(person));
    }finally{
      reader.close();
    }

    params = new AvroColumnReader.Params(trv2File);
    params.setSchema(Person.SCHEMA$);
    params.setModel(SpecificData.get());
    reader = new AvroColumnReader<Person>(params);

    try{
      Person readPerson = reader.next();
      assertThat(readPerson, is(person));
    }finally{
      reader.close();
    }
  }

  @Test
  public void toAvroTrevniKeyTargetReadSource() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(TrevniKeyPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target trevniFile = new TrevniKeyTarget(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, trevniFile);
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    PCollection<Person> retrievedPeople = pipeline.read(new TrevniKeySource<Person>(
        new Path(outputFile.toURI()), Avros.records(Person.class)));

    Person retrievedPerson = retrievedPeople.materialize().iterator().next();

    assertThat(retrievedPerson, is(person));

    File trvFile = new File(outputFile, "part-m-00000.trv-part-0.trv");

    AvroColumnReader.Params params = new AvroColumnReader.Params(trvFile);
    params.setSchema(Person.SCHEMA$);
    params.setModel(SpecificData.get());
    AvroColumnReader<Person> reader = new AvroColumnReader<Person>(params);

    try{
      Person readPerson = reader.next();
      assertThat(readPerson, is(person));
    }finally{
      reader.close();
    }
  }
}
