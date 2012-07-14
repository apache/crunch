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
import static org.junit.Assert.assertSame;

import java.util.List;

import org.apache.crunch.Pair;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.PGroupedTableType;
import org.junit.Test;

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

    PGroupedTableType<Integer, Person> groupedTableType = Avros.tableOf(Avros.ints(), Avros.reflects(Person.class))
        .getGroupedTableType();

    Pair<Integer, Iterable<Person>> detachedPair = groupedTableType.getDetachedValue(pair);

    assertSame(integerValue, detachedPair.first());
    List<Person> personList = Lists.newArrayList(detachedPair.second());
    assertEquals(inputPersonIterable, personList);
    assertNotSame(person, personList.get(0));

  }

}
