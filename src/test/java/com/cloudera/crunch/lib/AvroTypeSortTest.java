/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.lib;

import static com.cloudera.crunch.types.avro.Avros.ints;
import static com.cloudera.crunch.types.avro.Avros.records;
import static com.cloudera.crunch.types.avro.Avros.strings;
import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.test.Person;
import com.google.common.collect.Lists;

/**
 * Test sorting Avro types by selected inner field
 */
public class AvroTypeSortTest implements Serializable {

	private static final long serialVersionUID = 1344118240353796561L;

	private transient File avroFile;

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

		MRPipeline pipeline = new MRPipeline(AvroTypeSortTest.class);

		Person ccc10 = createPerson("CCC", 10);
		Person bbb20 = createPerson("BBB", 20);
		Person aaa30 = createPerson("AAA", 30);

		writeAvroFile(Lists.newArrayList(ccc10, bbb20, aaa30), avroFile);

		PCollection<Person> unsorted = pipeline.read(At.avroFile(
				avroFile.getAbsolutePath(), records(Person.class)));

		// Sort by Name
		MapFn<Person, String> nameExtractor = new MapFn<Person, String>() {

			@Override
			public String map(Person input) {
				return input.getName().toString();
			}
		};

		PCollection<Person> sortedByName = unsorted
				.by(nameExtractor, strings()).groupByKey().ungroup().values();

		List<Person> sortedByNameList = Lists.newArrayList(sortedByName
				.materialize());

		assertEquals(3, sortedByNameList.size());
		assertEquals(aaa30, sortedByNameList.get(0));
		assertEquals(bbb20, sortedByNameList.get(1));
		assertEquals(ccc10, sortedByNameList.get(2));

		// Sort by Age

		MapFn<Person, Integer> ageExtractor = new MapFn<Person, Integer>() {

			@Override
			public Integer map(Person input) {
				return input.getAge();
			}
		};

		PCollection<Person> sortedByAge = unsorted.by(ageExtractor, ints())
				.groupByKey().ungroup().values();

		List<Person> sortedByAgeList = Lists.newArrayList(sortedByAge
				.materialize());

		assertEquals(3, sortedByAgeList.size());
		assertEquals(ccc10, sortedByAgeList.get(0));
		assertEquals(bbb20, sortedByAgeList.get(1));
		assertEquals(aaa30, sortedByAgeList.get(2));

		pipeline.done();
	}

	private void writeAvroFile(List<Person> people, File avroFile)
			throws IOException {

		FileOutputStream outputStream = new FileOutputStream(avroFile);
		SpecificDatumWriter<Person> writer = new SpecificDatumWriter<Person>(
				Person.class);

		DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(
				writer);
		dataFileWriter.create(Person.SCHEMA$, outputStream);
		for (Person person : people) {
			dataFileWriter.append(person);
		}
		dataFileWriter.close();
		outputStream.close();
	}

	private Person createPerson(String name, int age) throws IOException {

		Person person = new Person();
		person.setAge(age);
		person.setName(name);
		List<CharSequence> siblingNames = Lists.newArrayList();
		person.setSiblingnames(siblingNames);

		return person;
	}
}
