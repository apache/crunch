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

import org.apache.crunch.Pair;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TupleDeepCopierTest {

  @Test
  public void testDeepCopy_Pair() {
    Person person = new Person();
    person.name = "John Doe";
    person.age = 42;
    person.siblingnames = Lists.<CharSequence> newArrayList();

    Pair<Integer, Person> inputPair = Pair.of(1, person);
    DeepCopier<Pair> deepCopier = new TupleDeepCopier<Pair>(
        Pair.class, Avros.ints(), Avros.records(Person.class));

    Pair<Integer, Person> deepCopyPair = deepCopier.deepCopy(inputPair);

    assertEquals(inputPair, deepCopyPair);
    assertNotSame(inputPair.second(), deepCopyPair.second());
  }

}
