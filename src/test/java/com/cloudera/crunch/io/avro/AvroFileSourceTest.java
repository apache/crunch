package com.cloudera.crunch.io.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.types.avro.AvroType;
import com.cloudera.crunch.types.avro.Avros;

public class AvroFileSourceTest {

	private Job job;
	File tempFile;

	@Before
	public void setUp() throws IOException {
		job = new Job();
		tempFile = File.createTempFile("test", ".avr");
	}

	@After
	public void tearDown() {
		tempFile.delete();
	}

	@Test
	public void testConfigureJob_SpecificData() throws IOException {
		AvroType<Person> avroSpecificType = Avros.records(Person.class);
		AvroFileSource<Person> personFileSource = new AvroFileSource<Person>(
				new Path(tempFile.getAbsolutePath()), avroSpecificType);

		personFileSource.configureSource(job, -1);

		assertFalse(job.getConfiguration().getBoolean(AvroJob.INPUT_IS_REFLECT,
				true));
		assertEquals(Person.SCHEMA$.toString(),
				job.getConfiguration().get(AvroJob.INPUT_SCHEMA));
	}

	@Test
	public void testConfigureJob_GenericData() throws IOException {
		AvroType<Record> avroGenericType = Avros.generics(Person.SCHEMA$);
		AvroFileSource<Record> personFileSource = new AvroFileSource<Record>(
				new Path(tempFile.getAbsolutePath()), avroGenericType);

		personFileSource.configureSource(job, -1);

		assertTrue(job.getConfiguration().getBoolean(AvroJob.INPUT_IS_REFLECT,
				false));

	}

}
