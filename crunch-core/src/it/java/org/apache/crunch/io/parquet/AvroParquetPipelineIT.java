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
package org.apache.crunch.io.parquet;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.Employee;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AvroParquetPipelineIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro.parquet");
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

  private void populateGenericParquetFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        new Path(avroFile.getPath()), schema);

    for (GenericRecord record : genericRecords) {
      writer.write(record);
    }

    writer.close();
  }

  @Test
  public void toAvroParquetFileTarget() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target parquetFileTarget = new AvroParquetFileTarget(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, parquetFileTarget);
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    Path parquetFile = new Path(new File(outputFile, "part-m-00000.parquet").getPath());

    AvroParquetReader<Person> reader = new AvroParquetReader<Person>(parquetFile);

    try {
      Person readPerson = reader.read();
      assertThat(readPerson, is(person));
    } finally {
      reader.close();
    }
  }

  @Test
  public void toAvroParquetFileTargetFromParquet() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericParquetFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(
        new AvroParquetFileSource<Person>(new Path(avroFile.getAbsolutePath()), Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target parquetFileTarget = new AvroParquetFileTarget(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, parquetFileTarget);
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    Path parquetFile = new Path(new File(outputFile, "part-m-00000.parquet").getPath());

    AvroParquetReader<Person> reader = new AvroParquetReader<Person>(parquetFile);

    try {
      Person readPerson = reader.read();
      assertThat(readPerson, is(person));
    } finally {
      reader.close();
    }
  }

  @Test
  public void toAvroParquetFileMultipleTarget() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));

    PCollection<Employee> employees = genericCollection.parallelDo(new DoFn<Person, Employee>() {
      @Override
      public void process(Person person, Emitter<Employee> emitter) {
        emitter.emit(new Employee(person.getName(), 0, "Eng"));
      }
    }, Avros.records(Employee.class));

    File output1File = tmpDir.getFile("output1");
    File output2File = tmpDir.getFile("output2");
    pipeline.write(genericCollection, new AvroParquetFileTarget(output1File.getAbsolutePath()));
    pipeline.write(employees, new AvroParquetFileSourceTarget(new Path(output2File.getAbsolutePath()),
        Avros.records(Employee.class)));
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();
    Employee employee = employees.materialize().iterator().next();

    Path parquet1File = new Path(new File(output1File, "part-m-00000.parquet").getPath());
    Path parquet2File = new Path(new File(output2File, "part-m-00000.parquet").getPath());

    AvroParquetReader<Person> personReader = new AvroParquetReader<Person>(parquet1File);

    try {
      Person readPerson = personReader.read();
      assertThat(readPerson, is(person));
    } finally {
      personReader.close();
    }

    AvroParquetReader<Employee> employeeReader = new AvroParquetReader<Employee>(parquet2File);

    try {
      Employee readEmployee = employeeReader.read();
      assertThat(readEmployee, is(employee));
    } finally {
      employeeReader.close();
    }

  }

  @Test
  public void toAvroParquetFileTargetReadSource() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroParquetPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target parquetFileTarget = new AvroParquetFileTarget(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, parquetFileTarget);
    pipeline.run();

    Person person = genericCollection.materialize().iterator().next();

    PCollection<Person> retrievedPeople = pipeline.read(new AvroParquetFileSource<Person>(
        new Path(outputFile.toURI()), Avros.records(Person.class)));

    Person retrievedPerson = retrievedPeople.materialize().iterator().next();

    assertThat(retrievedPerson, is(person));

    Path parquetFile = new Path(new File(outputFile, "part-m-00000.parquet").getPath());

    AvroParquetReader<Person> reader = new AvroParquetReader<Person>(parquetFile);

    try {
      Person readPerson = reader.read();
      assertThat(readPerson, is(person));
    } finally {
      reader.close();
    }
  }
}
