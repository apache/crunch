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
import static org.junit.Assert.assertNotNull;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.crunch.Tuple3;
import org.apache.crunch.TupleN;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class PTypeUtilsTest {
  @Test
  public void testPrimitives() {
    assertEquals(Avros.strings(), AvroTypeFamily.getInstance().as(Writables.strings()));
    Assert.assertEquals(Writables.doubles(), WritableTypeFamily.getInstance().as(Avros.doubles()));
  }

  @Test
  public void testTuple3() {
    PType<Tuple3<String, Float, Integer>> t = Writables.triples(Writables.strings(), Writables.floats(),
        Writables.ints());
    PType<Tuple3<String, Float, Integer>> at = AvroTypeFamily.getInstance().as(t);
    assertEquals(Avros.strings(), at.getSubTypes().get(0));
    assertEquals(Avros.floats(), at.getSubTypes().get(1));
    assertEquals(Avros.ints(), at.getSubTypes().get(2));
  }

  @Test
  public void testTupleN() {
    PType<TupleN> t = Avros.tuples(Avros.strings(), Avros.floats(), Avros.ints());
    PType<TupleN> wt = WritableTypeFamily.getInstance().as(t);
    assertEquals(Writables.strings(), wt.getSubTypes().get(0));
    assertEquals(Writables.floats(), wt.getSubTypes().get(1));
    assertEquals(Writables.ints(), wt.getSubTypes().get(2));
  }

  @Test
  public void testWritableCollections() {
    PType<Collection<String>> t = Avros.collections(Avros.strings());
    t = WritableTypeFamily.getInstance().as(t);
    assertEquals(Writables.strings(), t.getSubTypes().get(0));
  }

  @Test
  public void testAvroCollections() {
    PType<Collection<Double>> t = Writables.collections(Writables.doubles());
    t = AvroTypeFamily.getInstance().as(t);
    assertEquals(Avros.doubles(), t.getSubTypes().get(0));
  }

  @Test
  public void testAvroRegistered() {
    AvroType<Utf8> at = new AvroType<Utf8>(Utf8.class, Schema.create(Schema.Type.STRING));
    Avros.register(Utf8.class, at);
    assertEquals(at, Avros.records(Utf8.class));
  }

  @Test
  public void testWritableBuiltin() {
    assertNotNull(Writables.records(Text.class));
  }
}
