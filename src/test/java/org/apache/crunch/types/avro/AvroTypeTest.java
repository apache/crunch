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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;

import org.apache.crunch.test.Person;
import com.google.common.collect.Lists;

public class AvroTypeTest {

	@Test
	public void testIsSpecific_SpecificData() {
		assertTrue(Avros.records(Person.class).isSpecific());
	}

	@Test
	public void testIsGeneric_SpecificData() {
		assertFalse(Avros.records(Person.class).isGeneric());
	}

	@Test
	public void testIsSpecific_GenericData() {
		assertFalse(Avros.generics(Person.SCHEMA$).isSpecific());
	}

	@Test
	public void testIsGeneric_GenericData() {
		assertTrue(Avros.generics(Person.SCHEMA$).isGeneric());
	}

	@Test
	public void testIsSpecific_NonAvroClass() {
		assertFalse(Avros.ints().isSpecific());
	}

	@Test
	public void testIsGeneric_NonAvroClass() {
		assertFalse(Avros.ints().isGeneric());
	}

	@Test
	public void testIsSpecific_SpecificAvroTable() {
		assertTrue(Avros.tableOf(Avros.strings(), Avros.records(Person.class))
				.isSpecific());
	}

	@Test
	public void testIsGeneric_SpecificAvroTable() {
		assertFalse(Avros.tableOf(Avros.strings(), Avros.records(Person.class))
				.isGeneric());
	}

	@Test
	public void testIsSpecific_GenericAvroTable() {
		assertFalse(Avros.tableOf(Avros.strings(),
				Avros.generics(Person.SCHEMA$)).isSpecific());
	}

	@Test
	public void testIsGeneric_GenericAvroTable() {
		assertTrue(Avros.tableOf(Avros.strings(),
				Avros.generics(Person.SCHEMA$)).isGeneric());
	}

  @Test
  public void testGetDetachedValue_AlreadyMappedAvroType() {
    Integer value = 42;
    Integer detachedValue = Avros.ints().getDetachedValue(value);
    assertSame(value, detachedValue);
  }

  @Test
  public void testGetDetachedValue_GenericAvroType() {
    AvroType<Record> genericType = Avros.generics(Person.SCHEMA$);
    GenericData.Record record = new GenericData.Record(Person.SCHEMA$);
    record.put("name", "name value");
    record.put("age", 42);
    record.put("siblingnames", Lists.newArrayList());

    Record detachedRecord = genericType.getDetachedValue(record);
    assertEquals(record, detachedRecord);
    assertNotSame(record, detachedRecord);
  }

  @Test
  public void testGetDetachedValue_SpecificAvroType() {
    AvroType<Person> specificType = Avros.records(Person.class);
    Person person = new Person();
    person.setName("name value");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Person detachedPerson = specificType.getDetachedValue(person);
    assertEquals(person, detachedPerson);
    assertNotSame(person, detachedPerson);
  }

  @Test
  public void testGetDetachedValue_ReflectAvroType() {
    AvroType<Person> reflectType = Avros.reflects(Person.class);
    Person person = new Person();
    person.setName("name value");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Person detachedPerson = reflectType.getDetachedValue(person);
    assertEquals(person, detachedPerson);
    assertNotSame(person, detachedPerson);
  }

}
