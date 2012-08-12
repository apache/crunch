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

import java.util.Map;

import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.Maps;

public class MapDeepCopierTest {

  @Test
  public void testDeepCopy() {
    StringWrapper stringWrapper = new StringWrapper("value");
    String key = "key";
    Map<String, StringWrapper> map = Maps.newHashMap();
    map.put(key, stringWrapper);

    MapDeepCopier<StringWrapper> deepCopier = new MapDeepCopier<StringWrapper>(Avros.reflects(StringWrapper.class));
    Map<String, StringWrapper> deepCopy = deepCopier.deepCopy(map);

    assertEquals(map, deepCopy);
    assertNotSame(map.get(key), deepCopy.get(key));
  }

}
