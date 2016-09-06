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
import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.pobject.CollectionPObject;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class CollectionPObjectIT {

  private static final int LINES_IN_SHAKES = 3285;

  private static final String FIRST_SHAKESPEARE_LINE =
      "The Tragedie of Macbeth";

  private static final String LAST_SHAKESPEARE_LINE =
      "FINIS. THE TRAGEDIE OF MACBETH.";

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testPObjectMRPipeline() throws IOException {
    runPObject(new MRPipeline(CollectionPObjectIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testAsCollectionMRPipeline() throws IOException {
    runAsCollection(new MRPipeline(CollectionPObjectIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testPObjectMemPipeline() throws IOException {
    runPObject(MemPipeline.getInstance());
  }

  @Test
  public void testAsCollectionMemPipeline() throws IOException {
    runAsCollection(MemPipeline.getInstance());
  }

  private PCollection<String> getPCollection(Pipeline pipeline) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    return shakespeare;
  }

  private void verifyLines(String[] lines) {
    assertEquals("Not enough lines in Shakespeare.", LINES_IN_SHAKES, lines.length);
    assertEquals("First line in Shakespeare is wrong.", FIRST_SHAKESPEARE_LINE, lines[0]);
    assertEquals("Last line in Shakespeare is wrong.", LAST_SHAKESPEARE_LINE,
        lines[lines.length - 1]);
  }

  public void runPObject(Pipeline pipeline) throws IOException {
    PCollection<String> shakespeare = getPCollection(pipeline);
    PObject<Collection<String>> linesP = new CollectionPObject<String>(shakespeare);
    String[] lines = new String[LINES_IN_SHAKES];
    lines = linesP.getValue().toArray(lines);
    verifyLines(lines);
  }

  public void runAsCollection(Pipeline pipeline) throws IOException {
    PCollection<String> shakespeare = getPCollection(pipeline);
    String[] lines = new String[LINES_IN_SHAKES];
    lines = shakespeare.asCollection().getValue().toArray(lines);
    verifyLines(lines);
  }
}
