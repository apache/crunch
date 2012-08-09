package org.apache.crunch.types;

import static org.junit.Assert.*;

import java.util.Collection;

import org.apache.crunch.test.Person;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CollectionDeepCopierTest {

  @Test
  public void testDeepCopy() {
    Person person = new Person();
    person.setAge(42);
    person.setName("John Smith");
    person.setSiblingnames(Lists.<CharSequence> newArrayList());

    Collection<Person> personCollection = Lists.newArrayList(person);
    CollectionDeepCopier<Person> collectionDeepCopier = new CollectionDeepCopier<Person>(Avros.records(Person.class));

    Collection<Person> deepCopyCollection = collectionDeepCopier.deepCopy(personCollection);

    assertEquals(personCollection, deepCopyCollection);
    assertNotSame(personCollection.iterator().next(), deepCopyCollection.iterator().next());
  }

}
