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
package org.apache.crunch.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.util.Collection;

import org.apache.crunch.test.Person;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CollectionDeepCopierTest {

  @Test
  public void testDeepCopy() {
    Person person = new Person();
    person.age = 42;
    person.name = "John Smith";
    person.siblingnames = Lists.<CharSequence> newArrayList();

    Collection<Person> personCollection = Lists.newArrayList(person);
    CollectionDeepCopier<Person> collectionDeepCopier = new CollectionDeepCopier<Person>(
        Avros.records(Person.class));
    collectionDeepCopier.initialize(new Configuration());

    Collection<Person> deepCopyCollection = collectionDeepCopier.deepCopy(personCollection);

    assertEquals(personCollection, deepCopyCollection);
    assertNotSame(personCollection.iterator().next(), deepCopyCollection.iterator().next());
  }

  @Test
  public void testNullDeepCopy() {
    CollectionDeepCopier<Person> collectionDeepCopier = new CollectionDeepCopier<Person>(
        Avros.records(Person.class));
    collectionDeepCopier.initialize(new Configuration());
    Collection<Person> nullCollection = null;
    assertNull(collectionDeepCopier.deepCopy(nullCollection));
  }
}
