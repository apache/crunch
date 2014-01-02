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
package org.apache.crunch.types.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class SafeAvroSerializationIT implements Serializable {
	@Rule
	public transient TemporaryPath tmpDir = TemporaryPaths.create();

	/**
	 * Test to prove CRUNCH-316 has been fixed
	 */
	@Test
	public void testMapBufferTooSmallException() throws IOException {
		Configuration configuration = tmpDir.getDefaultConfiguration();

		// small io.sort.mb to make the test run faster with less resources
		configuration.set("io.sort.mb", "1");

		Pipeline pipeline = new MRPipeline(SafeAvroSerializationIT.class,
				configuration);

		Schema schema = new Schema.Parser().parse(tmpDir
				.copyResourceFile("CRUNCH-316.avsc"));

		PTable<String, GenericData.Record> leftSide = pipeline.read(
				At.avroFile(
						new Path(populateLeftSide(schema).getAbsolutePath()),
						Avros.generics(schema))).by(
				new MapFn<GenericData.Record, String>() {
					@Override
					public String map(GenericData.Record input) {
						return (String) input.get("tag").toString();
					}
				}, Avros.strings());

		PTable<String, String> rightSide = pipeline.read(
				At.avroFile(new Path(populateRightSide().getAbsolutePath()),
						Avros.strings())).by(new MapFn<String, String>() {
			@Override
			public String map(String input) {
				return input;
			}
		}, Avros.strings());

		PTable<String, org.apache.crunch.Pair<GenericData.Record, String>> joinedTable = leftSide
				.join(rightSide);

		// if CRUNCH-316 isn't fixed, this will result in an
		// ArrayIndexOutOfBoundsException in the reduce
		Collection<Pair<String, Pair<Record, String>>> joinRows = joinedTable
				.asCollection().getValue();

		assertEquals(1, joinRows.size());
		Pair<String, Pair<Record, String>> firstRow = joinRows.iterator()
				.next();
		assertEquals("c", firstRow.first());
		assertEquals("c", firstRow.second().first().get("tag").toString());
		assertEquals(createString('c', 40),
				firstRow.second().first().get("data1").toString());
		assertEquals(null, firstRow.second().first().get("data2"));
		assertEquals("c", firstRow.second().second());
	}

	private File populateLeftSide(Schema schema) throws IOException {
		File file = tmpDir.getFile("leftSide.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);

		GenericRecord record = new GenericData.Record(schema);

		// RECORD 1
		record.put("tag", "b");
		record.put("data1", createString('b', 996100));

		// buffer space has to run out on a write of less than 512 bytes for the
		// issue to occur
		record.put("data2", createString('b', 250));

		dataFileWriter.append(record);

		// RECORD 2 -- this record will be corrupted with overflow from RECORD 1
		record.put("tag", "c");
		record.put("data1", createString('c', 40));
		record.put("data2", null);
		dataFileWriter.append(record);

		dataFileWriter.close();
		return file;
	}

	private File populateRightSide() throws IOException {
		File file = tmpDir.getFile("rightSide.avro");
		DatumWriter<String> datumWriter = new GenericDatumWriter<String>(Avros
				.strings().getSchema());
		DataFileWriter<String> dataFileWriter = new DataFileWriter<String>(
				datumWriter);
		dataFileWriter.create(Avros.strings().getSchema(), file);

		// will join successfully to RECORD 2 from left side
		dataFileWriter.append("c");

		dataFileWriter.close();
		return file;
	}

	private static String createString(Character ch, int len) {
		StringBuilder buffer = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			buffer.append(ch);
		}
		return buffer.toString();
	}
}
