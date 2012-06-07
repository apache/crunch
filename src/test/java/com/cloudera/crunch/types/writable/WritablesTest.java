/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.types.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.Lists;

public class WritablesTest {

  @Test
  public void testNulls() throws Exception {
    Void n = null;
    NullWritable nw = NullWritable.get();
    testInputOutputFn(Writables.nulls(), n, nw);
  }

  @Test
  public void testStrings() throws Exception {
    String s = "abc";
    Text text = new Text(s);
    testInputOutputFn(Writables.strings(), s, text);
  }
  
  @Test
  public void testInts() throws Exception {
    int j = 55;
    IntWritable w = new IntWritable(j);
    testInputOutputFn(Writables.ints(), j, w);
  }
  
  @Test
  public void testLongs() throws Exception {
    long j = 55;
    LongWritable w = new LongWritable(j);
    testInputOutputFn(Writables.longs(), j, w);
  }
  
  @Test
  public void testFloats() throws Exception {
    float j = 55.5f;
    FloatWritable w = new FloatWritable(j);
    testInputOutputFn(Writables.floats(), j, w);
  }
  
  @Test
  public void testDoubles() throws Exception {
    double j = 55.5d;
    DoubleWritable w = new DoubleWritable(j);
    testInputOutputFn(Writables.doubles(), j, w);
  }
  
  @Test
  public void testBoolean() throws Exception {
    boolean j = false;
    BooleanWritable w = new BooleanWritable(j);
    testInputOutputFn(Writables.booleans(), j, w);
  }
  
  @Test
  public void testBytes() throws Exception {
    byte[] bytes = new byte[] { 17, 26, -98 };
    BytesWritable bw = new BytesWritable(bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    testInputOutputFn(Writables.bytes(), bb, bw);
  }
  
  @Test
  public void testCollections() throws Exception {
    String s = "abc";
    Collection<String> j = Lists.newArrayList();
    j.add(s);
    GenericArrayWritable<Text> w = new GenericArrayWritable<Text>(Text.class);
    w.set(new Text[]{
        new Text(s)
    });
    testInputOutputFn(Writables.collections(Writables.strings()), j, w);
  }
  
  @Test
  public void testPairs() throws Exception {
    Pair<String, String> j = Pair.of("a", "b");
    TupleWritable w = new TupleWritable(new Text[] {
        new Text("a"),
        new Text("b"),
    });
    w.setWritten(0);
    w.setWritten(1);
    testInputOutputFn(Writables.pairs(Writables.strings(), Writables.strings()), j, w);
  }
  
  @Test
  public void testNestedTables() throws Exception {
	PTableType<Long, Long> pll = Writables.tableOf(Writables.longs(), Writables.longs());
	PTableType<Pair<Long, Long>, String> nest = Writables.tableOf(pll, Writables.strings());
	assertNotNull(nest);
  }
  
  @Test
  public void testPairEquals() throws Exception {
	PType<Pair<Long, ByteBuffer>> t1 = Writables.pairs(Writables.longs(), Writables.bytes());
	PType<Pair<Long, ByteBuffer>> t2 = Writables.pairs(Writables.longs(), Writables.bytes());
	assertEquals(t1, t2);
	assertEquals(t1.hashCode(), t2.hashCode());
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testTriples() throws Exception {
    Tuple3 j = Tuple3.of("a", "b", "c");
    TupleWritable w = new TupleWritable(new Text[] {
        new Text("a"),
        new Text("b"),
        new Text("c"),
    });
    w.setWritten(0);
    w.setWritten(1);
    w.setWritten(2);
    WritableType<?, ?> wt = Writables.triples(Writables.strings(),
        Writables.strings(), Writables.strings());
    testInputOutputFn(wt, j, w);
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testQuads() throws Exception {
    Tuple4 j = Tuple4.of("a", "b", "c", "d");
    TupleWritable w = new TupleWritable(new Text[] {
        new Text("a"),
        new Text("b"),
        new Text("c"),
        new Text("d"),
    });
    w.setWritten(0);
    w.setWritten(1);
    w.setWritten(2);
    w.setWritten(3);
    WritableType<?, ?> wt = Writables.quads(Writables.strings(), Writables.strings(),
        Writables.strings(), Writables.strings());
    testInputOutputFn(wt, j, w);
  }
  
  @Test
  public void testTupleN() throws Exception {
    TupleN j = new TupleN("a", "b", "c", "d", "e");
    TupleWritable w = new TupleWritable(new Text[] {
        new Text("a"),
        new Text("b"),
        new Text("c"),
        new Text("d"),
        new Text("e"),
    });
    w.setWritten(0);
    w.setWritten(1);
    w.setWritten(2);
    w.setWritten(3);
    w.setWritten(4);
    WritableType<?, ?> wt = Writables.tuples(Writables.strings(), Writables.strings(), 
        Writables.strings(), Writables.strings(), Writables.strings());
    testInputOutputFn(wt, j, w);
  }
  
  protected static class TestWritable implements Writable {
    String left;
    int right;
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(left);
      out.writeInt(right);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      left = in.readUTF();
      right = in.readInt();
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TestWritable other = (TestWritable) obj;
      if (left == null) {
        if (other.left != null)
          return false;
      } else if (!left.equals(other.left))
        return false;
      if (right != other.right)
        return false;
      return true;
    }
    
  }
  @Test
  public void testRecords() throws Exception {
    TestWritable j = new TestWritable();
    j.left = "a";
    j.right = 1;
    TestWritable w = new TestWritable();
    w.left = "a";
    w.right = 1;
    WritableType<?, ?> wt = Writables.records(TestWritable.class);
    testInputOutputFn(wt, j, w);
  }
  
  @Test
  public void testTableOf() throws Exception {
    Pair<String, String> j = Pair.of("a", "b");
    Pair<Text, Text> w = Pair.of(new Text("a"), new Text("b"));
    WritableTableType<String, String> wtt = Writables.tableOf(Writables.strings(), Writables.strings());
    testInputOutputFn(wtt, j, w);
  }

  @Test
  public void testRegister() throws Exception {
    WritableType<TestWritable, TestWritable> wt = Writables.writables(TestWritable.class);
    Writables.register(TestWritable.class, wt);
    assertTrue(Writables.records(TestWritable.class) == wt);
  }
    
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static void testInputOutputFn(PType ptype, Object java, Object writable) {
    ptype.getInputMapFn().initialize();
    ptype.getOutputMapFn().initialize();
    assertEquals(java, ptype.getInputMapFn().map(writable));
    assertEquals(writable, ptype.getOutputMapFn().map(java));
  }
}
