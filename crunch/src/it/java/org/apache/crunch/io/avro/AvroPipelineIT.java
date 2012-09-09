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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroPipelineIT implements Serializable {

  private transient File avroFile;
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Before
  public void setUp() throws IOException {
    avroFile = tmpDir.getFile("test.avro");
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
  public void toTextShouldWriteAvroDataAsDatumText() throws Exception {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), Person.SCHEMA$);

    Pipeline pipeline = new MRPipeline(AvroFileSourceTargetIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));
    File outputFile = tmpDir.getFile("output");
    Target textFile = To.textFile(outputFile.getAbsolutePath());
    pipeline.write(genericCollection, textFile);
    pipeline.run();
    Person person = genericCollection.materialize().iterator().next();
    String outputString = FileUtils.readFileToString(new File(outputFile, "part-m-00000"));
    assertTrue(outputString.contains(person.toString()));
  }
}
