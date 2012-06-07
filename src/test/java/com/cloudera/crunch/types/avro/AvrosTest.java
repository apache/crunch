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
package com.cloudera.crunch.types.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.writable.Writables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * TODO test Avros.register and Avros.containers
 */
public class AvrosTest {

  @Test
  public void testNulls() throws Exception {
    Void n = null;
    testInputOutputFn(Avros.nulls(), n, n);
  }
  
  @Test
  public void testStrings() throws Exception {
    String s = "abc";
    Utf8 w = new Utf8(s);
    testInputOutputFn(Avros.strings(), s, w);
  }
  
  @Test
  public void testInts() throws Exception {
    int j = 55;
    testInputOutputFn(Avros.ints(), j, j);
  }
  @Test
  public void testLongs() throws Exception {
    long j = Long.MAX_VALUE;
    testInputOutputFn(Avros.longs(), j, j);
  }
  @Test
  public void testFloats() throws Exception {
    float j = Float.MIN_VALUE;
    testInputOutputFn(Avros.floats(), j, j);
  }
  @Test
  public void testDoubles() throws Exception {
    double j = Double.MIN_VALUE;
    testInputOutputFn(Avros.doubles(), j, j);
  }
  
  @Test
  public void testBooleans() throws Exception {
    boolean j = true;
    testInputOutputFn(Avros.booleans(), j, j);
  }
  
  @Test
  public void testBytes() throws Exception {
    byte[] bytes = new byte[] { 17, 26, -98 };
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    testInputOutputFn(Avros.bytes(), bb, bb);
  }

  @Test
  public void testCollections() throws Exception {
    Collection<String> j = Lists.newArrayList();
    j.add("a");
    j.add("b");
    Schema collectionSchema = Schema.createArray(
        Schema.createUnion(ImmutableList.of(
            Avros.strings().getSchema(), Schema.create(Type.NULL))));
    GenericData.Array<Utf8> w = new GenericData.Array<Utf8>(2, collectionSchema);
    w.add(new Utf8("a"));
    w.add(new Utf8("b"));
    testInputOutputFn(Avros.collections(Avros.strings()), j, w);
  }
  
  @Test
  public void testNestedTables() throws Exception {
	PTableType<Long, Long> pll = Avros.tableOf(Avros.longs(), Avros.longs());
	PTableType<Pair<Long, Long>, String> nest = Avros.tableOf(pll, Avros.strings());
	assertNotNull(nest);
  }
  
  @Test
  public void testPairs() throws Exception {
    AvroType<Pair<String, String>> at = Avros.pairs(Avros.strings(), Avros.strings());
    Pair<String, String> j = Pair.of("a", "b");
    GenericData.Record w = new GenericData.Record(at.getSchema());
    w.put(0, new Utf8("a"));
    w.put(1, new Utf8("b"));
    testInputOutputFn(at, j, w);
  }
  
  @Test
  public void testPairEquals() throws Exception {
	AvroType<Pair<Long, ByteBuffer>> at1 = Avros.pairs(Avros.longs(), Avros.bytes());
	AvroType<Pair<Long, ByteBuffer>> at2 = Avros.pairs(Avros.longs(), Avros.bytes());
	assertEquals(at1, at2);
	assertEquals(at1.hashCode(), at2.hashCode());
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testTriples() throws Exception {
    AvroType at = Avros.triples(Avros.strings(), Avros.strings(), Avros.strings());
    Tuple3 j = Tuple3.of("a", "b", "c");
    GenericData.Record w = new GenericData.Record(at.getSchema());
    w.put(0, new Utf8("a"));
    w.put(1, new Utf8("b"));
    w.put(2, new Utf8("c"));
    testInputOutputFn(at, j, w);
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testQuads() throws Exception {
    AvroType at = Avros.quads(Avros.strings(), Avros.strings(), Avros.strings(), Avros.strings());
    Tuple4 j = Tuple4.of("a", "b", "c", "d");
    GenericData.Record w = new GenericData.Record(at.getSchema());
    w.put(0, new Utf8("a"));
    w.put(1, new Utf8("b"));
    w.put(2, new Utf8("c"));
    w.put(3, new Utf8("d"));
    testInputOutputFn(at, j, w);
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testTupleN() throws Exception {
    AvroType at = Avros.tuples(Avros.strings(), Avros.strings(), Avros.strings(), Avros.strings(),
        Avros.strings());
    TupleN j = new TupleN("a", "b", "c", "d", "e");
    GenericData.Record w = new GenericData.Record(at.getSchema());
    w.put(0, new Utf8("a"));
    w.put(1, new Utf8("b"));
    w.put(2, new Utf8("c"));
    w.put(3, new Utf8("d"));
    w.put(4, new Utf8("e"));
    testInputOutputFn(at, j, w);
    
  }
   
  @Test
  @SuppressWarnings("rawtypes")
  public void testWritables() throws Exception {
    AvroType at = Avros.writables(LongWritable.class);
    LongWritable lw = new LongWritable(1729L);
    assertEquals(lw, at.getInputMapFn().map(at.getOutputMapFn().map(lw)));
  }
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testTableOf() throws Exception {
    AvroType at = Avros.tableOf(Avros.strings(), Avros.strings());
    Pair<String, String> j = Pair.of("a", "b");
    org.apache.avro.mapred.Pair w = new org.apache.avro.mapred.Pair(at.getSchema());
    w.put(0, new Utf8("a"));
    w.put(1, new Utf8("b"));
    // TODO update this after resolving the o.a.a.m.Pair.equals issue
    initialize(at);
    assertEquals(j, at.getInputMapFn().map(w));
    org.apache.avro.mapred.Pair converted =
        (org.apache.avro.mapred.Pair) at.getOutputMapFn().map(j);
    assertEquals(w.key(), converted.key());
    assertEquals(w.value(), converted.value());
  }
  
  private static void initialize(PType ptype) {
    ptype.getInputMapFn().initialize();
    ptype.getOutputMapFn().initialize();
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static void testInputOutputFn(PType ptype, Object java, Object avro) {
    initialize(ptype);
    assertEquals(java, ptype.getInputMapFn().map(avro));
    assertEquals(avro, ptype.getOutputMapFn().map(java));
  }
}
