/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.types.avro.Avros;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class AvroFileSourceTargetTest implements Serializable {

	private transient File avroFile;

	@Before
	public void setUp() throws IOException {
		avroFile = File.createTempFile("test", ".avro");
	}

	@After
	public void tearDown() {
		avroFile.delete();
	}

	private void populateGenericFile(List<GenericRecord> genericRecords) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(this.avroFile);
		GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(
				Person.SCHEMA$);

		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				genericDatumWriter);
		dataFileWriter.create(Person.SCHEMA$, outputStream);

		for (GenericRecord record : genericRecords) {
			dataFileWriter.append(record);
		}

		dataFileWriter.close();
		outputStream.close();

	}

	@Test
	public void testSpecific() throws IOException {
		GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
		savedRecord.put("name", "John Doe");
		savedRecord.put("age", 42);
		savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
		populateGenericFile(Lists.newArrayList(savedRecord));

		Pipeline pipeline = new MRPipeline(AvroFileSourceTargetTest.class);
		PCollection<Person> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
				Avros.records(Person.class)));

		List<Person> personList = Lists.newArrayList(genericCollection.materialize());

		Person expectedPerson = new Person();
		expectedPerson.setName("John Doe");
		expectedPerson.setAge(42);

		List<CharSequence> siblingNames = Lists.newArrayList();
		siblingNames.add("Jimmy");
		siblingNames.add("Jane");
		expectedPerson.setSiblingnames(siblingNames);

		assertEquals(Lists.newArrayList(expectedPerson), Lists.newArrayList(personList));
	}

	@Test
	public void testGeneric() throws IOException {
		String genericSchemaJson = Person.SCHEMA$.toString().replace("Person", "GenericPerson");
		Schema genericPersonSchema = new Schema.Parser().parse(genericSchemaJson);
		GenericRecord savedRecord = new GenericData.Record(genericPersonSchema);
		savedRecord.put("name", "John Doe");
		savedRecord.put("age", 42);
		savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
		populateGenericFile(Lists.newArrayList(savedRecord));

		Pipeline pipeline = new MRPipeline(AvroFileSourceTargetTest.class);
		PCollection<Record> genericCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
				Avros.generics(genericPersonSchema)));

		List<Record> recordList = Lists.newArrayList(genericCollection.materialize());

		assertEquals(Lists.newArrayList(savedRecord), Lists.newArrayList(recordList));
	}
}
