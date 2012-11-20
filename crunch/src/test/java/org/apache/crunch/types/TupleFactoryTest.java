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

import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.junit.Test;

public class TupleFactoryTest {

  @Test
  public void testGetTupleFactory_Pair() {
    assertEquals(TupleFactory.PAIR, TupleFactory.getTupleFactory(Pair.class));
  }

  @Test
  public void testGetTupleFactory_Tuple3() {
    assertEquals(TupleFactory.TUPLE3, TupleFactory.getTupleFactory(Tuple3.class));
  }

  @Test
  public void testGetTupleFactory_Tuple4() {
    assertEquals(TupleFactory.TUPLE4, TupleFactory.getTupleFactory(Tuple4.class));
  }

  @Test
  public void testGetTupleFactory_TupleN() {
    assertEquals(TupleFactory.TUPLEN, TupleFactory.getTupleFactory(TupleN.class));
  }

  public void testGetTupleFactory_CustomTupleClass() {
	TupleFactory<CustomTupleImplementation> customTupleFactory = TupleFactory.create(CustomTupleImplementation.class);
    assertEquals(customTupleFactory, TupleFactory.getTupleFactory(CustomTupleImplementation.class));
  }

  private static class CustomTupleImplementation implements Tuple {

    @Override
    public Object get(int index) {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }

  }
}
