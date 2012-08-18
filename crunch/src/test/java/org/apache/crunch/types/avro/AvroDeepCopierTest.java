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
import static org.junit.Assert.assertNotSame;

import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.avro.AvroDeepCopier.AvroSpecificDeepCopier;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroDeepCopierTest {

  @Test
  public void testDeepCopySpecific() {
    Person person = new Person();
    person.name = "John Doe";
    person.age = 42;
    person.siblingnames = Lists.<CharSequence> newArrayList();

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
  public void testDeepCopyReflect() {
    ReflectedPerson person = new ReflectedPerson();
    person.name = "John Doe";
    person.age = 42;
    person.siblingnames = Lists.newArrayList();

    ReflectedPerson deepCopyPerson = new AvroDeepCopier.AvroReflectDeepCopier<ReflectedPerson>(
        ReflectedPerson.class, Avros.reflects(ReflectedPerson.class).getSchema()).deepCopy(person);

    assertEquals(person, deepCopyPerson);
    assertNotSame(person, deepCopyPerson);

  }

}
