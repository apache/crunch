package com.cloudera.crunch.types.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.test.Person;
import com.google.common.collect.Lists;

public class AvroTableTypeTest {

  @Test
  public void testGetDetachedValue() {
    Integer integerValue = 42;
    Person person = new Person();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Pair<Integer, Person> pair = Pair.of(integerValue, person);

    AvroTableType<Integer, Person> tableType = Avros.tableOf(Avros.ints(),
        Avros.reflects(Person.class));

    Pair<Integer, Person> detachedPair = tableType.getDetachedValue(pair);

    assertSame(integerValue, detachedPair.first());
    assertEquals(person, detachedPair.second());
    assertNotSame(person, detachedPair.second());
  }

}
