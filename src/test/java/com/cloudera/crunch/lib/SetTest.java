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
package com.cloudera.crunch.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class SetTest {
  
  private PTypeFamily typeFamily;
  
  private Pipeline pipeline;
  private PCollection<String> set1;
  private PCollection<String> set2;
  
  public SetTest(PTypeFamily typeFamily) {
    this.typeFamily = typeFamily;
  }
  
  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { WritableTypeFamily.getInstance() },
        { AvroTypeFamily.getInstance() }
    };
    return Arrays.asList(data);
  }
  
  @Before
  public void setUp() throws IOException {
    String set1InputPath = FileHelper.createTempCopyOf("set1.txt");
    String set2InputPath = FileHelper.createTempCopyOf("set2.txt");
    pipeline = new MRPipeline(SetTest.class);
    set1 = pipeline.read(At.textFile(set1InputPath, typeFamily.strings()));
    set2 = pipeline.read(At.textFile(set2InputPath, typeFamily.strings()));
  }
  
  @After
  public void tearDown() {
    pipeline.done();
  }
  
  @Test
  public void testDifference() throws Exception {
    PCollection<String> difference = Set.difference(set1, set2);
    assertEquals(Lists.newArrayList("b", "e"),
        Lists.newArrayList(difference.materialize()));
  }
  
  @Test
  public void testIntersection() throws Exception {
    PCollection<String> intersection = Set.intersection(set1, set2);
    assertEquals(Lists.newArrayList("a", "c"),
        Lists.newArrayList(intersection.materialize()));
  }
  
  @Test
  public void testComm() throws Exception {
    PCollection<Tuple3<String, String, String>> comm = Set.comm(set1, set2);
    Iterator<Tuple3<String, String, String>> i = comm.materialize().iterator();
    checkEquals(null, null, "a", i.next());
    checkEquals("b", null, null, i.next());
    checkEquals(null, null, "c", i.next());
    checkEquals(null, "d", null, i.next());
    checkEquals("e", null, null, i.next());
    assertFalse(i.hasNext());
  }

  private void checkEquals(String s1, String s2, String s3,
      Tuple3<String, String, String> tuple) {
    assertEquals("first string", s1, tuple.first());
    assertEquals("second string", s2, tuple.second());
    assertEquals("third string", s3, tuple.third());
  }

}
