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

import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class WriteModeTest {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test(expected=CrunchRuntimeException.class)
  public void testDefault() throws Exception {
    run(null, true);
  }

  @Test(expected=CrunchRuntimeException.class)
  public void testDefaultNoRun() throws Exception {
    run(null, false);
  }
  
  @Test
  public void testOverwrite() throws Exception {
    Path p = run(WriteMode.OVERWRITE, true);
    PCollection<String> lines = MemPipeline.getInstance().readTextFile(p.toString());
    assertEquals(ImmutableList.of("some", "string", "values"), lines.materialize());
  }
  
  @Test(expected=CrunchRuntimeException.class)
  public void testOverwriteNoRun() throws Exception {
    run(WriteMode.OVERWRITE, false);
  }
  
  @Test
  public void testAppend() throws Exception {
    Path p = run(WriteMode.APPEND, true);
    PCollection<String> lines = MemPipeline.getInstance().readTextFile(p.toString());
    assertEquals(ImmutableList.of("some", "string", "values", "some", "string", "values"),
        lines.materialize());
  }
  
  @Test
  public void testAppendNoRun() throws Exception {
    Path p = run(WriteMode.APPEND, false);
    PCollection<String> lines = MemPipeline.getInstance().readTextFile(p.toString());
    assertEquals(ImmutableList.of("some", "string", "values", "some", "string", "values"),
        lines.materialize());
  }
  
  Path run(WriteMode writeMode, boolean doRun) throws Exception {
    Path output = tmpDir.getPath("existing");
    FileSystem fs = FileSystem.get(tmpDir.getDefaultConfiguration());
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    Pipeline p = MemPipeline.getInstance();
    PCollection<String> data = MemPipeline.typedCollectionOf(Avros.strings(),
        ImmutableList.of("some", "string", "values"));
    data.write(To.textFile(output));

    if (doRun) {
      p.run();
    }
    
    if (writeMode == null) {
      data.write(To.textFile(output));
    } else {
      data.write(To.textFile(output), writeMode);
    }
    
    p.run();
    
    return output;
  }
}
