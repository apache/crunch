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

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class WritableTypeTest {

  @Test
  public void testGetDetachedValue_AlreadyMappedWritable() {
    WritableType<String, Text> stringType = Writables.strings();
    String value = "test";
    assertSame(value, stringType.getDetachedValue(value));
  }

  @Test
  public void testGetDetachedValue_CustomWritable() {
    WritableType<Text, Text> textWritableType = Writables.writables(Text.class);
    Text value = new Text("test");

    Text detachedValue = textWritableType.getDetachedValue(value);
    assertEquals(value, detachedValue);
    assertNotSame(value, detachedValue);
  }

}
