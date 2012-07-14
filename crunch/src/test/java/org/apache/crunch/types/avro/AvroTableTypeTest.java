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

import org.apache.crunch.Pair;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.junit.Test;

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

    AvroTableType<Integer, Person> tableType = Avros.tableOf(Avros.ints(), Avros.reflects(Person.class));

    Pair<Integer, Person> detachedPair = tableType.getDetachedValue(pair);

    assertSame(integerValue, detachedPair.first());
    assertEquals(person, detachedPair.second());
    assertNotSame(person, detachedPair.second());
  }

  @Test
  public void testIsReflect_ContainsReflectKey() {
    assertTrue(Avros.tableOf(Avros.reflects(StringWrapper.class), Avros.ints()).isReflect());
  }

  @Test
  public void testIsReflect_ContainsReflectValue() {
    assertTrue(Avros.tableOf(Avros.ints(), Avros.reflects(StringWrapper.class)).isReflect());
  }

  @Test
  public void testReflect_NoReflectKeyOrValue() {
    assertFalse(Avros.tableOf(Avros.ints(), Avros.ints()).isReflect());
  }

}
