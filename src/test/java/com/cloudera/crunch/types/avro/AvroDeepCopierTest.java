package com.cloudera.crunch.types.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;

import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.types.avro.AvroDeepCopier.AvroSpecificDeepCopier;
import com.google.common.collect.Lists;

public class AvroDeepCopierTest {

  @Test
  public void testDeepCopySpecific() {
    Person person = new Person();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Person deepCopyPerson = new AvroSpecificDeepCopier<Person>(Person.class, Person.SCHEMA$)
        .deepCopy(person);

    assertEquals(person, deepCopyPerson);
    assertNotSame(person, deepCopyPerson);
  }

  @Test
  public void testDeepCopyGeneric() {
    Record record = new Record(Person.SCHEMA$);
    record.put("name", "John Doe");
    record.put("age", 42);
    record.put("siblingnames", Lists.newArrayList());

    Record deepCopyRecord = new AvroDeepCopier.AvroGenericDeepCopier(Person.SCHEMA$)
        .deepCopy(record);

    assertEquals(record, deepCopyRecord);
    assertNotSame(record, deepCopyRecord);
  }

  @Test
  public void testDeepCopyReflect() {
    Person person = new Person();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Person deepCopyPerson = new AvroDeepCopier.AvroReflectDeepCopier<Person>(Person.class,
        Person.SCHEMA$).deepCopy(person);

    assertEquals(person, deepCopyPerson);
    assertNotSame(person, deepCopyPerson);

  }

}
