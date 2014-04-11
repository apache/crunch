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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.crunch.Pair;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class WritableTypeTest {

  @Test(expected = IllegalStateException.class)
  public void testGetDetachedValue_NotInitialized() {
    WritableType<Text, Text> textWritableType = Writables.writables(Text.class);
    Text value = new Text("test");

    // Calling getDetachedValue without first calling initialize should throw an
    // exception
    textWritableType.getDetachedValue(value);
  }

  @Test
  public void testGetDetachedValue_CustomWritable() {
    WritableType<Text, Text> textWritableType = Writables.writables(Text.class);
    textWritableType.initialize(new Configuration());
    Text value = new Text("test");

    Text detachedValue = textWritableType.getDetachedValue(value);
    assertEquals(value, detachedValue);
    assertNotSame(value, detachedValue);
  }

  @Test
  public void testGetDetachedValue_Collection() {
    Collection<Text> textCollection = Lists.newArrayList(new Text("value"));
    WritableType<Collection<Text>, GenericArrayWritable> ptype = Writables
        .collections(Writables.writables(Text.class));
    ptype.initialize(new Configuration());

    Collection<Text> detachedCollection = ptype.getDetachedValue(textCollection);
    assertEquals(textCollection, detachedCollection);
    assertNotSame(textCollection.iterator().next(), detachedCollection.iterator().next());
  }

  @Test
  public void testGetDetachedValue_Tuple() {
    Pair<Text, Text> textPair = Pair.of(new Text("one"), new Text("two"));
    WritableType<Pair<Text, Text>, TupleWritable> ptype = Writables.pairs(
        Writables.writables(Text.class), Writables.writables(Text.class));
    ptype.initialize(new Configuration());

    Pair<Text, Text> detachedPair = ptype.getDetachedValue(textPair);
    assertEquals(textPair, detachedPair);
    assertNotSame(textPair.first(), detachedPair.first());
    assertNotSame(textPair.second(), detachedPair.second());
  }

  @Test
  public void testGetDetachedValue_Map() {
    Map<String, Text> stringTextMap = Maps.newHashMap();
    stringTextMap.put("key", new Text("value"));

    WritableType<Map<String, Text>, MapWritable> ptype = Writables.maps(Writables
        .writables(Text.class));
    ptype.initialize(new Configuration());
    Map<String, Text> detachedMap = ptype.getDetachedValue(stringTextMap);

    assertEquals(stringTextMap, detachedMap);
    assertNotSame(stringTextMap.get("key"), detachedMap.get("key"));
  }

  @Test
  public void testGetDetachedValue_String() {
    String s = "test";
    WritableType<String, Text> stringType = Writables.strings();
    stringType.initialize(new Configuration());
    String detached = stringType.getDetachedValue(s);

    assertSame(s, detached);
  }


  @Test
  public void testGetDetachedValue_Primitive() {
    WritableType<Integer, IntWritable> intType = Writables.ints();
    intType.initialize(new Configuration());
    Integer intValue = Integer.valueOf(42);
    Integer detachedValue = intType.getDetachedValue(intValue);
    assertSame(intValue, detachedValue);
  }

  @Test
  public void testGetDetachedValue_NonPrimitive() {
    WritableType<Text, Text> textType = Writables.writables(Text.class);
    textType.initialize(new Configuration());
    Text text = new Text("test");
    Text detachedValue = textType.getDetachedValue(text);
    assertEquals(text, detachedValue);
    assertNotSame(text, detachedValue);
  }

  @Test
  public void testGetDetachedValue_ImmutableDerived() {
    PType<UUID> uuidType = PTypes.uuid(WritableTypeFamily.getInstance());
    uuidType.initialize(new Configuration());

    UUID uuid = new UUID(1L, 1L);
    UUID detached = uuidType.getDetachedValue(uuid);

    assertSame(uuid, detached);
  }

  @Test
  public void testGetDetachedValue_MutableDerived() {
    PType<StringWrapper> jsonType = PTypes.jsonString(StringWrapper.class, WritableTypeFamily.getInstance());
    jsonType.initialize(new Configuration());

    StringWrapper stringWrapper = new StringWrapper();
    stringWrapper.setValue("test");

    StringWrapper detachedValue = jsonType.getDetachedValue(stringWrapper);

    assertNotSame(stringWrapper, detachedValue);
    assertEquals(stringWrapper, detachedValue);
  }

  @Test
  public void testGetDetachedValue_Bytes() {
    byte[] buffer = new byte[]{1, 2, 3};
    WritableType<ByteBuffer,BytesWritable> byteType = Writables.bytes();
    byteType.initialize(new Configuration());

    ByteBuffer detachedValue = byteType.getDetachedValue(ByteBuffer.wrap(buffer));

    byte[] detachedBuffer = new byte[buffer.length];
    detachedValue.get(detachedBuffer);

    assertArrayEquals(buffer, detachedBuffer);
    buffer[0] = 99;
    assertEquals(detachedBuffer[0], 1);
  }
}
