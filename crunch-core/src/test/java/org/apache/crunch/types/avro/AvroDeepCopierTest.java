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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroDeepCopier.AvroSpecificDeepCopier;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class AvroDeepCopierTest {
  
  @Test
  public void testDeepCopySpecific() {
    Person person = new Person();
    person.name = "John Doe";
    person.age = 42;
    person.siblingnames = Lists.<CharSequence> newArrayList();

    Person deepCopyPerson = new AvroSpecificDeepCopier<Person>(Person.SCHEMA$).deepCopy(person);

    assertEquals(person, deepCopyPerson);
    assertNotSame(person, deepCopyPerson);
  }

  @Test
  public void testDeepCopySpecific_Null() {
    assertNull(new AvroSpecificDeepCopier<Person>(Person.SCHEMA$).deepCopy(null));
  }

  @Test
  public void testDeepCopyGeneric() {
    Record record = new Record(Person.SCHEMA$);
    record.put("name", "John Doe");
    record.put("age", 42);
    record.put("siblingnames", Lists.newArrayList());

    Record deepCopyRecord = new AvroDeepCopier.AvroGenericDeepCopier(Person.SCHEMA$).deepCopy(record);

    assertEquals(record, deepCopyRecord);
    assertNotSame(record, deepCopyRecord);
  }

  @Test
  public void testDeepCopyGeneric_Null() {
    assertNull(new AvroDeepCopier.AvroGenericDeepCopier(Person.SCHEMA$).deepCopy(null));
  }

  @Test
  public void testDeepCopyReflect() {
    ReflectedPerson person = new ReflectedPerson();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<String>newArrayList());

    AvroDeepCopier<ReflectedPerson> avroDeepCopier = new AvroDeepCopier.AvroReflectDeepCopier<ReflectedPerson>(
        ReflectedPerson.class, Avros.reflects(ReflectedPerson.class).getSchema());
    avroDeepCopier.initialize(new Configuration());

    ReflectedPerson deepCopyPerson = avroDeepCopier.deepCopy(person);

    assertEquals(person, deepCopyPerson);
    assertNotSame(person, deepCopyPerson);

  }

  @Test
  public void testSerializableReflectPType() throws Exception {
    ReflectedPerson person = new ReflectedPerson();
    person.setName("John Doe");
    person.setAge(42);
    person.setSiblingnames(Lists.<String>newArrayList());

    PType<ReflectedPerson> rptype = Avros.reflects(ReflectedPerson.class);
    rptype.initialize(new Configuration());
    ReflectedPerson copy1 = rptype.getDetachedValue(person);
    assertEquals(copy1, person);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(rptype);
    oos.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    rptype = (PType<ReflectedPerson>) ois.readObject();

    rptype.initialize(new Configuration());
    ReflectedPerson copy2 = rptype.getDetachedValue(person);
    assertEquals(person, copy2);
  }

  @Test
  public void testDeepCopyReflect_Null() {
    AvroDeepCopier<ReflectedPerson> avroDeepCopier = new AvroDeepCopier.AvroReflectDeepCopier<ReflectedPerson>(
        ReflectedPerson.class, Avros.reflects(ReflectedPerson.class).getSchema());
    avroDeepCopier.initialize(new Configuration());

    assertNull(avroDeepCopier.deepCopy(null));
  }

  @Test
  public void testDeepCopy_ByteBuffer() {
    byte[] bytes = new byte[] { 1, 2, 3 };
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    ByteBuffer deepCopied = new AvroDeepCopier.AvroByteBufferDeepCopier().INSTANCE.deepCopy(buffer);

    // Change the original array to make sure we've really got a copy
    bytes[0] = 0;
    assertArrayEquals(new byte[] { 1, 2, 3 }, deepCopied.array());

  }
}
