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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Map;

import org.apache.crunch.Pair;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class WritableTypeTest {

  @Test
  public void testGetDetachedValue_CustomWritable() {
    WritableType<Text, Text> textWritableType = Writables.writables(Text.class);
    Text value = new Text("test");

    Text detachedValue = textWritableType.getDetachedValue(value);
    assertEquals(value, detachedValue);
    assertNotSame(value, detachedValue);
  }

  @Test
  public void testGetDetachedValue_Collection() {
    Collection<Text> textCollection = Lists.newArrayList(new Text("value"));
    WritableType<Collection<Text>, GenericArrayWritable<Text>> ptype = Writables.collections(Writables
        .writables(Text.class));

    Collection<Text> detachedCollection = ptype.getDetachedValue(textCollection);
    assertEquals(textCollection, detachedCollection);
    assertNotSame(textCollection.iterator().next(), detachedCollection.iterator().next());
  }

  @Test
  public void testGetDetachedValue_Tuple() {
    Pair<Text, Text> textPair = Pair.of(new Text("one"), new Text("two"));
    WritableType<Pair<Text, Text>, TupleWritable> ptype = Writables.pairs(Writables.writables(Text.class),
        Writables.writables(Text.class));
    ptype.getOutputMapFn().initialize();
    ptype.getInputMapFn().initialize();

    Pair<Text, Text> detachedPair = ptype.getDetachedValue(textPair);
    assertEquals(textPair, detachedPair);
    assertNotSame(textPair.first(), detachedPair.first());
    assertNotSame(textPair.second(), detachedPair.second());
  }

  @Test
  public void testGetDetachedValue_Map() {
    Map<String, Text> stringTextMap = Maps.newHashMap();
    stringTextMap.put("key", new Text("value"));

    WritableType<Map<String, Text>, MapWritable> ptype = Writables.maps(Writables.writables(Text.class));
    Map<String, Text> detachedMap = ptype.getDetachedValue(stringTextMap);

    assertEquals(stringTextMap, detachedMap);
    assertNotSame(stringTextMap.get("key"), detachedMap.get("key"));
  }

}
