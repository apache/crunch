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
package org.apache.crunch.types.orc;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.crunch.Pair;
import org.apache.crunch.TupleN;
import org.apache.crunch.types.TupleFactory;
import org.apache.crunch.types.orc.TupleObjectInspector.ByteBufferObjectInspector;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TupleObjectInspectorTest {
  
  @Test
  public void testTupleObjectInspector() {
    // test get
    TupleObjectInspector<TupleN> toi = new TupleObjectInspector<TupleN>(TupleFactory.TUPLEN, 
        Writables.strings(), Writables.ints(), Writables.floats());
    TupleN tuple = new TupleN("Alice", 28, 165.2f);
    List<Object> values = toi.getStructFieldsDataAsList(tuple);
    assertEquals("Alice", values.get(0));
    assertEquals(28, values.get(1));
    assertEquals(165.2f, values.get(2));
    
    // test create
    TupleN newTuple = toi.create("Alice", 28, 165.2f);
    assertEquals(tuple, newTuple);
    TupleObjectInspector<Pair> poi = new TupleObjectInspector<Pair>(TupleFactory.PAIR,
        Writables.strings(), Writables.ints());
    Pair pair = poi.create("word", 29);
    assertEquals("word", pair.first());
    assertEquals(29, pair.second());
  }
  
  @Test
  public void testByteBufferObjectInspector() {
    byte[] bytes = {0, 9, 4, 18, 64, 6, 1};
    BytesWritable bw = new BytesWritable(bytes);
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    ByteBufferObjectInspector bboi = new ByteBufferObjectInspector();
    
    assertArrayEquals(bytes, bboi.getPrimitiveJavaObject(buf));
    assertEquals(bw, bboi.getPrimitiveWritableObject(buf));
    assertEquals(buf, bboi.create(bytes));
    assertEquals(buf, bboi.create(bw));
    
    ByteBuffer newBuf = bboi.copyObject(buf);
    assertTrue(buf != newBuf);
    assertEquals(buf, newBuf);
  }

}
