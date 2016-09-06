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
import java.lang.String;

import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.pobject.FirstElementPObject;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class FirstElementPObjectIT {

  private static final String FIRST_SHAKESPEARE_LINE =
      "The Tragedie of Macbeth";

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMRPipeline() throws IOException {
    run(new MRPipeline(FirstElementPObjectIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testInMemoryPipeline() throws IOException {
    run(MemPipeline.getInstance());
  }

  public void run(Pipeline pipeline) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    PObject<String> firstLine = new FirstElementPObject<String>(shakespeare);
    String first = firstLine.getValue();
    assertEquals("First line in Shakespeare is wrong.", FIRST_SHAKESPEARE_LINE, first);
  }
}
