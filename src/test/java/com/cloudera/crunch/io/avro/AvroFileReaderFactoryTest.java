package com.cloudera.crunch.io.avro;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.types.avro.Avros;
import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;

public class AvroFileReaderFactoryTest {

	private File avroFile;
	private Schema schema;

	@Before
	public void setUp() throws IOException {
		InputSupplier<InputStream> inputStreamSupplier = newInputStreamSupplier(getResource("person.avro"));
		schema = new Schema.Parser().parse(inputStreamSupplier.getInput());
		avroFile = File.createTempFile("test", ".av");
	}

	@After
	public void tearDown() {
		avroFile.delete();
	}

	private void populateGenericFile(List<GenericRecord> genericRecords)
			throws IOException {
		FileOutputStream outputStream = new FileOutputStream(this.avroFile);
		GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(
				schema);

		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				genericDatumWriter);
		dataFileWriter.create(schema, outputStream);

		for (GenericRecord record : genericRecords) {
			dataFileWriter.append(record);
		}

		dataFileWriter.close();
		outputStream.close();

	}

	@Test
	public void testRead_GenericReader() throws IOException {
		GenericRecord savedRecord = new GenericData.Record(schema);
		savedRecord.put("name", "John Doe");
		savedRecord.put("age", 42);
		savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
		populateGenericFile(Lists.newArrayList(savedRecord));

		AvroFileReaderFactory<GenericData.Record> genericReader = new AvroFileReaderFactory<GenericData.Record>(
				Avros.generics(schema), new Configuration());
		Iterator<GenericData.Record> recordIterator = genericReader.read(
				FileSystem.getLocal(new Configuration()),
				new Path(this.avroFile.getAbsolutePath()));

		GenericRecord genericRecord = recordIterator.next();
		assertEquals(savedRecord, genericRecord);
		assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testRead_SpecificReader() throws IOException {
		GenericRecord savedRecord = new GenericData.Record(schema);
		savedRecord.put("name", "John Doe");
		savedRecord.put("age", 42);
		savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
		populateGenericFile(Lists.newArrayList(savedRecord));

		AvroFileReaderFactory<Person> genericReader = new AvroFileReaderFactory<Person>(
				Avros.records(Person.class), new Configuration());
		Iterator<Person> recordIterator = genericReader.read(
				FileSystem.getLocal(new Configuration()),
				new Path(this.avroFile.getAbsolutePath()));

		Person expectedPerson = new Person();
		expectedPerson.setAge(42);
		expectedPerson.setName("John Doe");
		List<CharSequence> siblingNames = Lists.newArrayList();
		siblingNames.add("Jimmy");
		siblingNames.add("Jane");
		expectedPerson.setSiblingnames(siblingNames);

		Person person = recordIterator.next();

		assertEquals(expectedPerson, person);
		assertFalse(recordIterator.hasNext());
	}
}
