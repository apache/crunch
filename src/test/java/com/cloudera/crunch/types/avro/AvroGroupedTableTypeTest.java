package com.cloudera.crunch.types.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.test.Person;
import com.cloudera.crunch.types.PGroupedTableType;
import com.google.common.collect.Lists;

public class AvroGroupedTableTypeTest {

  @Test
  public void testGetDetachedValue() {
    Integer integerValue = 42;
    Person person = new Person();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Iterable<Person> inputPersonIterable = Lists.newArrayList(person);
    Pair<Integer, Iterable<Person>> pair = Pair.of(integerValue, inputPersonIterable);

    PGroupedTableType<Integer, Person> groupedTableType = Avros.tableOf(Avros.ints(),
        Avros.reflects(Person.class)).getGroupedTableType();

    Pair<Integer, Iterable<Person>> detachedPair = groupedTableType.getDetachedValue(pair);

    assertSame(integerValue, detachedPair.first());
    List<Person> personList = Lists.newArrayList(detachedPair.second());
    assertEquals(inputPersonIterable, personList);
    assertNotSame(person, personList.get(0));

  }

}
