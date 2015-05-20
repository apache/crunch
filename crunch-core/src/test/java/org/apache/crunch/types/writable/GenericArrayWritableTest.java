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

import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.crunch.test.Tests;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;


public class GenericArrayWritableTest {

  @Test
  public void testEmpty() {
    GenericArrayWritable src = new GenericArrayWritable();
    src.set(new BytesWritable[0]);

    GenericArrayWritable dest = Tests.roundtrip(src, new GenericArrayWritable());

    assertThat(dest.get().length, is(0));
  }

  @Test
  public void testNonEmpty() {
    GenericArrayWritable src = new GenericArrayWritable();
    src.set(new BytesWritable[] {
        new BytesWritable("foo".getBytes(Charset.forName("UTF-8"))),
        new BytesWritable("bar".getBytes(Charset.forName("UTF-8"))) });

    GenericArrayWritable dest = Tests.roundtrip(src, new GenericArrayWritable());

    assertThat(src.get(), not(sameInstance(dest.get())));
    assertThat(dest.get().length, is(2));
    assertThat(Arrays.asList(dest.get()),
        hasItems(new BytesWritable("foo".getBytes(Charset.forName("UTF-8"))),
                 new BytesWritable("bar".getBytes(Charset.forName("UTF-8")))));
  }

  @Test
  public void testNulls() {
    GenericArrayWritable src = new GenericArrayWritable();
    src.set(new BytesWritable[] {
        new BytesWritable("a".getBytes(Charset.forName("UTF-8"))), null,
        new BytesWritable("b".getBytes(Charset.forName("UTF-8"))) });

    GenericArrayWritable dest = Tests.roundtrip(src, new GenericArrayWritable());

    assertThat(src.get(), not(sameInstance(dest.get())));
    assertThat(dest.get().length, is(3));
    assertThat(Arrays.asList(dest.get()),
        hasItems(new BytesWritable("a".getBytes(Charset.forName("UTF-8"))),
                 new BytesWritable("b".getBytes(Charset.forName("UTF-8"))), null));
  }

}
