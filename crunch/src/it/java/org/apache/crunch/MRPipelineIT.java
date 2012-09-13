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

import java.io.File;
import java.io.Serializable;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
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
    PCollection<String> filter = genericCollection.filter("Filtering data", new FilterFn<String>() {
      @Override
      public boolean accept(String input) {
        return true;
      }
    });
    filter.materialize();
    pipeline.run();
    File file = tmpDir.getFile("output.txt");
    Target outFile = To.textFile(file.getAbsolutePath());
    PCollection<String> write = filter.write(outFile);
    write.materialize();
    pipeline.run();
  }

}
