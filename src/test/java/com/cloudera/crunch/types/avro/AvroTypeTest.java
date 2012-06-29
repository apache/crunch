package com.cloudera.crunch.types.avro;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudera.crunch.test.Person;

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

}
