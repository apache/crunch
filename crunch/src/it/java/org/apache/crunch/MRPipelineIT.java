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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

public class MRPipelineIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void materializedColShouldBeWritten() throws Exception {
    File textFile = tmpDir.copyResourceFile("shakes.txt");
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> genericCollection = pipeline.readTextFile(textFile.getAbsolutePath());
    pipeline.run();
    PCollection<String> filter = genericCollection.filter("Filtering data", FilterFns.<String>ACCEPT_ALL());
    filter.materialize();
    pipeline.run();
    File file = tmpDir.getFile("output.txt");
    Target outFile = To.textFile(file.getAbsolutePath());
    PCollection<String> write = filter.write(outFile);
    write.materialize();
    pipeline.run();
  }
  
  
  
  @Test
  public void testPGroupedTableToMultipleOutputs() throws IOException{
    Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());
    PGroupedTable<String, String> groupedLineTable = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt")).by(IdentityFn.<String>getInstance(), Writables.strings()).groupByKey();
    
    PTable<String, String> ungroupedTableA = groupedLineTable.ungroup();
    PTable<String, String> ungroupedTableB = groupedLineTable.ungroup();
    
    File outputDirA = tmpDir.getFile("output_a");
    File outputDirB = tmpDir.getFile("output_b");
    
    pipeline.writeTextFile(ungroupedTableA, outputDirA.getAbsolutePath());
    pipeline.writeTextFile(ungroupedTableB, outputDirB.getAbsolutePath());
    pipeline.done();

    // Verify that output from a single PGroupedTable can be sent to multiple collections
    assertTrue(new File(outputDirA, "part-r-00000").exists());
    assertTrue(new File(outputDirB, "part-r-00000").exists());
  }

}
