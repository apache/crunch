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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.Integer;
import java.lang.Iterable;
import java.lang.String;
import java.util.Iterator;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class PObjectsIT {

  private static final Integer LINES_IN_SHAKES = 3285;

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  /**
   * A mock PObject that should map PCollections of strings to an integer count of the number of
   * elements in the underlying PCollection.
   */
  public static class MockPObjectImpl extends PObjectImpl<String, Integer> {
    private int numProcessCalls;

    public MockPObjectImpl(PCollection<String> collect) {
      super(collect);
      numProcessCalls = 0;
    }

    @Override
    public Integer process(Iterable<String> input) {
      numProcessCalls++;
      int i = 0;
      Iterator<String> itr = input.iterator();
      while (itr.hasNext()) {
        i++;
        itr.next();
      }
      return i;
    }

    public int getNumProcessCalls() {
      return numProcessCalls;
    }
  }

  @Test
  public void testMRPipeline() throws IOException {
    run(new MRPipeline(PObjectsIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testInMemoryPipeline() throws IOException {
    run(MemPipeline.getInstance());
  }

  public void run(Pipeline pipeline) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    MockPObjectImpl lineCount = new MockPObjectImpl(shakespeare);
    // Get the line count once and verify it's correctness.
    assertEquals("Incorrect number of lines counted from PCollection.", LINES_IN_SHAKES,
        lineCount.getValue());
    // And do it again.
    assertEquals("Incorrect number of lines counted from PCollection.", LINES_IN_SHAKES,
        lineCount.getValue());
    // Make sure process was called only once because the PObject's value was cached after the
    // first call.
    assertEquals("Process on PObject not called exactly 1 times.", 1,
        lineCount.getNumProcessCalls());
  }
}
