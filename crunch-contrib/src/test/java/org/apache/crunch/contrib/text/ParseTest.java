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
package org.apache.crunch.contrib.text;

import static org.apache.crunch.contrib.text.Extractors.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.contrib.text.Parse;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 *
 */
public class ParseTest {

  @Test
  public void testInt() {
    assertEquals(Integer.valueOf(1729), xint().extract("1729"));
    assertEquals(Integer.valueOf(321), xint(321).extract("foo"));
  }

  @Test
  public void testString() {
    assertEquals("bar", xstring().extract("bar"));
  }

  @Test
  public void testPairWithDrop() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(",").drop(0, 2).build();
    assertEquals(Pair.of(1, "abc"), xpair(sf, xint(), xstring()).extract("foo,1,17.29,abc"));
  }

  @Test
  public void testTripsWithSkip() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(";").skip("^foo").build();
    assertEquals(Tuple3.of(17, "abc", 3.4f),
        xtriple(sf, xint(), xstring(), xfloat()).extract("foo17;abc;3.4"));
  }
  
  @Test
  public void testTripsWithKeep() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(";").keep(1, 2, 3).build();
    assertEquals(Tuple3.of(17, "abc", 3.4f),
        xtriple(sf, xint(), xstring(), xfloat()).extract("foo;17;abc;3.4"));
  }
  
  @Test
  public void testQuadsWithWhitespace() {
    TokenizerFactory sf = TokenizerFactory.getDefaultInstance();
    assertEquals(Tuple4.of(1.3, "foo", true, 1L),
        xquad(sf, xdouble(), xstring(), xboolean(), xlong()).extract("1.3   foo  true 1"));
  }
  
  @Test
  public void testTupleN() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(",").build();
    assertEquals(new TupleN(1, false, true, 2, 3),
        xtupleN(sf, xint(), xboolean(), xboolean(), xint(), xint()).extract("1,false,true,2,3"));
  }
  
  @Test
  public void testCollections() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(";").build();
    // Use 3000 as the default for values we can't parse
    Extractor<Collection<Integer>> x = xcollect(sf, xint(3000));
    
    assertEquals(ImmutableList.of(1, 2, 3), x.extract("1;2;3"));
    assertFalse(x.errorOnLastRecord());
    assertEquals(ImmutableList.of(17, 29, 3000), x.extract("17;29;a"));
    assertTrue(x.errorOnLastRecord());
    assertEquals(1, x.getStats().getErrorCount());
  }
  
  @Test
  public void testNestedComposites() {
    TokenizerFactory outer = TokenizerFactory.builder().delimiter(";").build();
    TokenizerFactory inner = TokenizerFactory.builder().delimiter(",").build();
    Extractor<Pair<Pair<Long, Integer>, Tuple3<String, Integer, Float>>> extractor =
        xpair(outer, xpair(inner, xlong(), xint()), xtriple(inner, xstring(), xint(), xfloat()));
    assertEquals(Pair.of(Pair.of(1L, 2), Tuple3.of("a", 17, 29f)),
        extractor.extract("1,2;a,17,29"));
  }
  
  @Test
  public void testParse() {
    TokenizerFactory sf = TokenizerFactory.builder().delimiter(",").build();
    PCollection<String> lines = MemPipeline.typedCollectionOf(Avros.strings(), "1,3.0");
    Iterable<Pair<Integer, Float>> it = Parse.parse("test", lines,
        xpair(sf, xint(), xfloat())).materialize();
    assertEquals(ImmutableList.of(Pair.of(1, 3.0f)), it);
  }
}
