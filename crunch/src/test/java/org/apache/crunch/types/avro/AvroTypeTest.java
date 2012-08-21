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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.Pair;
import org.apache.crunch.TupleN;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AvroTypeTest {

  @Test
  public void testIsSpecific_SpecificData() {
    assertTrue(Avros.records(Person.class).hasSpecific());
  }

  @Test
  public void testIsGeneric_SpecificData() {
    assertFalse(Avros.records(Person.class).isGeneric());
  }

  @Test
  public void testIsSpecific_GenericData() {
    assertFalse(Avros.generics(Person.SCHEMA$).hasSpecific());
  }

  @Test
  public void testIsGeneric_GenericData() {
    assertTrue(Avros.generics(Person.SCHEMA$).isGeneric());
  }

  @Test
  public void testIsSpecific_NonAvroClass() {
    assertFalse(Avros.ints().hasSpecific());
  }

  @Test
  public void testIsGeneric_NonAvroClass() {
    assertFalse(Avros.ints().isGeneric());
  }

  @Test
  public void testIsSpecific_SpecificAvroTable() {
    assertTrue(Avros.tableOf(Avros.strings(), Avros.records(Person.class)).hasSpecific());
  }

  @Test
  public void testIsGeneric_SpecificAvroTable() {
    assertFalse(Avros.tableOf(Avros.strings(), Avros.records(Person.class)).isGeneric());
  }

  @Test
  public void testIsSpecific_GenericAvroTable() {
    assertFalse(Avros.tableOf(Avros.strings(), Avros.generics(Person.SCHEMA$)).hasSpecific());
  }

  @Test
  public void testIsGeneric_GenericAvroTable() {
    assertFalse(Avros.tableOf(Avros.strings(), Avros.generics(Person.SCHEMA$)).isGeneric());
  }

  @Test
  public void testIsReflect_GenericType() {
    assertFalse(Avros.generics(Person.SCHEMA$).hasReflect());
  }

  @Test
  public void testIsReflect_SpecificType() {
    assertFalse(Avros.records(Person.class).hasReflect());
  }

  @Test
  public void testIsReflect_ReflectSimpleType() {
    assertTrue(Avros.reflects(StringWrapper.class).hasReflect());
  }

  @Test
  public void testIsReflect_NonReflectSubType() {
    assertFalse(Avros.pairs(Avros.ints(), Avros.ints()).hasReflect());
  }

  @Test
  public void testIsReflect_ReflectSubType() {
    assertTrue(Avros.pairs(Avros.ints(), Avros.reflects(StringWrapper.class)).hasReflect());
  }

  @Test
  public void testIsReflect_TableOfNonReflectTypes() {
    assertFalse(Avros.tableOf(Avros.ints(), Avros.strings()).hasReflect());
  }

  @Test
  public void testIsReflect_TableWithReflectKey() {
    assertTrue(Avros.tableOf(Avros.reflects(StringWrapper.class), Avros.ints()).hasReflect());
  }

  @Test
  public void testIsReflect_TableWithReflectValue() {
    assertTrue(Avros.tableOf(Avros.ints(), Avros.reflects(StringWrapper.class)).hasReflect());
  }

  @Test
  public void testReflect_CollectionContainingReflectValue() {
    assertTrue(Avros.collections(Avros.reflects(StringWrapper.class)).hasReflect());
  }

  @Test
  public void testReflect_CollectionNotContainingReflectValue() {
    assertFalse(Avros.collections(Avros.generics(Person.SCHEMA$)).hasReflect());
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
  
  private Person createPerson(){
    Person person = new Person();
    person.name = "name value";
    person.age = 42;
    person.siblingnames = Lists.<CharSequence> newArrayList();
    return person;
  }

  @Test
  public void testGetDetachedValue_SpecificAvroType() {
    AvroType<Person> specificType = Avros.specifics(Person.class);
    Person person = createPerson();
    Person detachedPerson = specificType.getDetachedValue(person);
    assertEquals(person, detachedPerson);
    assertNotSame(person, detachedPerson);
  }

  static class ReflectedPerson {
    String name;
    int age;
    List<String> siblingnames;
    
    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof ReflectedPerson)) {
        return false;
      }
      ReflectedPerson that = (ReflectedPerson) other;
      return name.equals(that.name)&& age == that.age && siblingnames.equals(that.siblingnames); 
    }
  }
  
  @Test
  public void testGetDetachedValue_ReflectAvroType() {
    AvroType<ReflectedPerson> reflectType = Avros.reflects(ReflectedPerson.class);
    ReflectedPerson rp = new ReflectedPerson();
    rp.name = "josh";
    rp.age = 32;
    rp.siblingnames = Lists.newArrayList();
    ReflectedPerson detached = reflectType.getDetachedValue(rp);
    assertEquals(rp, detached);
    assertNotSame(rp, detached);
  }

  @Test
  public void testGetDetachedValue_Pair() {
    Person person = createPerson();
    AvroType<Pair<Integer, Person>> pairType = Avros.pairs(Avros.ints(),
        Avros.records(Person.class));

    Pair<Integer, Person> inputPair = Pair.of(1, person);
    Pair<Integer, Person> detachedPair = pairType.getDetachedValue(inputPair);

    assertEquals(inputPair, detachedPair);
    assertNotSame(inputPair.second(), detachedPair.second());
  }
  
  @Test
  public void testGetDetachedValue_Collection(){
    Person person = createPerson();
    List<Person> personList = Lists.newArrayList(person);
    
    AvroType<Collection<Person>> collectionType = Avros.collections(Avros.records(Person.class));
    
    Collection<Person> detachedCollection = collectionType.getDetachedValue(personList);
    
    assertEquals(personList, detachedCollection);
    Person detachedPerson = detachedCollection.iterator().next();
    
    assertNotSame(person, detachedPerson);
  }
  
  @Test
  public void testGetDetachedValue_Map(){
    String key = "key";
    Person value = createPerson();
    
    Map<String,Person> stringPersonMap = Maps.newHashMap();
    stringPersonMap.put(key, value);
    
    AvroType<Map<String, Person>> mapType = Avros.maps(Avros.records(Person.class));
    
    Map<String, Person> detachedMap = mapType.getDetachedValue(stringPersonMap);
    
    assertEquals(stringPersonMap, detachedMap);
    assertNotSame(value, detachedMap.get(key));
  }
  
  @Test
  public void testGetDetachedValue_TupleN(){
    Person person = createPerson();
    AvroType<TupleN> ptype = Avros.tuples(Avros.records(Person.class));
    TupleN tuple = new TupleN(person);
    TupleN detachedTuple = ptype.getDetachedValue(tuple);
    
    assertEquals(tuple, detachedTuple);
    assertNotSame(person, detachedTuple.get(0));
  }
  
}
