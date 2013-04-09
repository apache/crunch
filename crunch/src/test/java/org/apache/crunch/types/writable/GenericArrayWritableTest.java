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

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import org.apache.crunch.test.Tests;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;


public class GenericArrayWritableTest {

  @Test
  public void testEmpty() {
    GenericArrayWritable<Text> src = new GenericArrayWritable<Text>(Text.class);
    src.set(new Text[0]);

    GenericArrayWritable<Text> dest = Tests.roundtrip(src, new GenericArrayWritable<Text>());

    assertThat(dest.get().length, is(0));
  }

  @Test
  public void testNonEmpty() {
    GenericArrayWritable<Text> src = new GenericArrayWritable<Text>(Text.class);
    src.set(new Text[] { new Text("foo"), new Text("bar") });

    GenericArrayWritable<Text> dest = Tests.roundtrip(src, new GenericArrayWritable<Text>());

    assertThat(src.get(), not(sameInstance(dest.get())));
    assertThat(dest.get().length, is(2));
    assertThat(Arrays.asList(dest.get()), hasItems((Writable) new Text("foo"), new Text("bar")));
  }

  @Test
  public void testNulls() {
    GenericArrayWritable<Text> src = new GenericArrayWritable<Text>(Text.class);
    src.set(new Text[] { new Text("a"), null, new Text("b") });

    GenericArrayWritable<Text> dest = Tests.roundtrip(src, new GenericArrayWritable<Text>());

    assertThat(src.get(), not(sameInstance(dest.get())));
    assertThat(dest.get().length, is(3));
    assertThat(Arrays.asList(dest.get()), hasItems((Writable) new Text("a"), new Text("b"), null));
  }

}
