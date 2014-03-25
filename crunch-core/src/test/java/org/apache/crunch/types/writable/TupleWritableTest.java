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
package org.apache.crunch.types.writable;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TupleWritableTest {

  @Test
  public void testSerialization() throws IOException {
    TupleWritable t1 = new TupleWritable(
        new Writable[] { new IntWritable(10), null, new Text("hello"), new Text("world") });
    TupleWritable t2 = new TupleWritable();
    t2.readFields(new DataInputStream(new ByteArrayInputStream(WritableUtils.toByteArray(t1))));
    assertTrue(t2.has(0));
    assertEquals(new IntWritable(10), t2.get(0));
    assertFalse(t2.has(1));
    assertNull(t2.get(1));
    assertTrue(t2.has(2));
    assertEquals(new Text("hello"), t2.get(2));
    assertTrue(t2.has(3));
    assertEquals(new Text("world"), t2.get(3));
  }

  @Test
  public void testCompare() throws IOException {
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        new TupleWritable(new Writable[] { new IntWritable(2) }),
        -1);
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        0);
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1), new IntWritable(1) }),
        new TupleWritable(new Writable[] { new IntWritable(1), new IntWritable(2) }),
        -1);
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1), new IntWritable(2) }),
        new TupleWritable(new Writable[] { new IntWritable(1), new IntWritable(2) }),
        0);
    doTestCompare(
        new TupleWritable(new Writable[] { null  }),
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        -1);
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        new TupleWritable(new Writable[] { new Text("1") }),
        1); // code for IntWritable is larger than code for Text
    doTestCompare(
        new TupleWritable(new Writable[] { new IntWritable(1) }),
        new TupleWritable(new Writable[] { new IntWritable(1), new IntWritable(2) }),
        -1); // shorter is less
  }

  private void doTestCompare(TupleWritable t1, TupleWritable t2, int result) throws IOException {
    // test comparing objects
    TupleWritable.Comparator comparator = TupleWritable.Comparator.getInstance();
    assertEquals(result, comparator.compare(t1, t2));

    // test comparing buffers
    DataOutputBuffer buffer1 = new DataOutputBuffer();
    DataOutputBuffer buffer2 = new DataOutputBuffer();
    t1.write(buffer1);
    t2.write(buffer2);
    assertEquals(result, comparator.compare(
        buffer1.getData(), 0, buffer1.getLength(),
        buffer2.getData(), 0, buffer2.getLength()));
  }
}
