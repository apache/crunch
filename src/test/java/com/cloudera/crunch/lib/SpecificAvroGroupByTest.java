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

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.test.Person.Builder;
import com.cloudera.crunch.types.avro.Avros;
import com.cloudera.crunch.types.avro.SafeAvroSerialization;
import com.google.common.collect.Lists;

/**
 * Test {@link SafeAvroSerialization} with Specific Avro types
 */
public class SpecificAvroGroupByTest implements Serializable {

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
	public void testGrouByWithSpecificAvroType() throws Exception {

		MRPipeline pipeline = new MRPipeline(SpecificAvroGroupByTest.class);

		testSpecificAvro(pipeline);
	}

	@Test(expected = Exception.class)
	public void testGrouByOnSpecificAvroButReflectionDatumReader()
			throws Exception {
		MRPipeline pipeline = new MRPipeline(SpecificAvroGroupByTest.class);

		// Simulate the old (pre-fix) AvroSerializer implementation which
		// creates ReflectDatumReader even for specific Avro types. This leads
		// to:
		// java.lang.ClassCastException: [Ljava.lang.String; cannot be cast to
		// java.util.List
		// at com.cloudera.crunch.test.Person.put(Person.java:49)
		pipeline.getConfiguration().setBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT,
				true);

		testSpecificAvro(pipeline);
	}

	public void testSpecificAvro(MRPipeline pipeline) throws Exception {

		createPersonAvroFile(avroFile);

		PCollection<Person> unsorted = pipeline.read(At.avroFile(
				avroFile.getAbsolutePath(), Avros.records(Person.class)));

		PTable<String, Person> sorted = unsorted
				.parallelDo(new MapFn<Person, Pair<String, Person>>() {

					@Override
					public Pair<String, Person> map(Person input) {
						String key = input.getName().toString();
						return Pair.of(key, input);

					}
				}, Avros.tableOf(Avros.strings(), Avros.records(Person.class)))
				.groupByKey().ungroup();

		List<Pair<String, Person>> outputPersonList = Lists.newArrayList(sorted
				.materialize());

		assertEquals(1, outputPersonList.size());
		assertEquals(String.class, outputPersonList.get(0).first().getClass());
		assertEquals(Person.class, outputPersonList.get(0).second().getClass());
		
		pipeline.done();
	}

	private void createPersonAvroFile(File avroFile) throws IOException {

		Builder person = Person.newBuilder();
		person.setAge(40);
		person.setName("Bob");
		List<CharSequence> siblingNames = Lists.newArrayList();
		siblingNames.add("Bob" + "1");
		siblingNames.add("Bob" + "2");
		person.setSiblingnames(siblingNames);

		FileOutputStream outputStream = new FileOutputStream(avroFile);
		SpecificDatumWriter<Person> writer = new SpecificDatumWriter<Person>(
				Person.class);

		DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(
				writer);
		dataFileWriter.create(Person.SCHEMA$, outputStream);
		dataFileWriter.append(person.build());
		dataFileWriter.close();
		outputStream.close();
	}
}
