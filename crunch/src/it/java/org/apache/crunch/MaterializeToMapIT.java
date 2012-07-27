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
package org.apache.crunch;

import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.FileHelper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class MaterializeToMapIT {

  static final ImmutableList<Pair<Integer, String>> kvPairs = ImmutableList.of(Pair.of(0, "a"), Pair.of(1, "b"),
      Pair.of(2, "c"), Pair.of(3, "e"));

  public void assertMatches(Map<Integer, String> m) {
    for (Integer k : m.keySet()) {
      System.out.println(k + " " + kvPairs.get(k).second() + " " + m.get(k));
      assertTrue(kvPairs.get(k).second().equals(m.get(k)));
    }
  }

  @Test
  public void testMemMaterializeToMap() {
    assertMatches(MemPipeline.tableOf(kvPairs).materializeToMap());
  }

  private static class Set1Mapper extends MapFn<String, Pair<Integer, String>> {
    @Override
    public Pair<Integer, String> map(String input) {

      int k = -1;
      if (input.equals("a"))
        k = 0;
      else if (input.equals("b"))
        k = 1;
      else if (input.equals("c"))
        k = 2;
      else if (input.equals("e"))
        k = 3;
      return Pair.of(k, input);
    }
  }
  @Rule
  public TemporaryPath temporaryPath= new TemporaryPath();

  @Test
  public void testMRMaterializeToMap() throws IOException {
    Pipeline p = new MRPipeline(MaterializeToMapIT.class, temporaryPath.setTempLoc(new Configuration()));
    String inputFile = FileHelper.createTempCopyOf("set1.txt");
    PCollection<String> c = p.readTextFile(inputFile);
    PTypeFamily tf = c.getTypeFamily();
    PTable<Integer, String> t = c.parallelDo(new Set1Mapper(), tf.tableOf(tf.ints(), tf.strings()));
    Map<Integer, String> m = t.materializeToMap();
    assertMatches(m);
  }

}