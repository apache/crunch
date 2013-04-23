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

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Files;

/**
 *
 */
public class CleanTextIT {

  private static final int LINES_IN_SHAKES = 3667;
  
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  static DoFn<String, String> CLEANER = new DoFn<String, String>() {
    @Override
    public void process(String input, Emitter<String> emitter) {
      emitter.emit(input.toLowerCase());
    }
  };
  
  static DoFn<String, String> SPLIT = new DoFn<String, String>() {
    @Override
    public void process(String input, Emitter<String> emitter) {
      for (String word : input.split("\\S+")) {
        if (!word.isEmpty()) {
          emitter.emit(word);
        }
      }
    }
  };
  
  @Test
  public void testMapSideOutputs() throws Exception {
    Pipeline pipeline = new MRPipeline(CleanTextIT.class, tmpDir.getDefaultConfiguration());
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    
    PCollection<String> cleanShakes = shakespeare.parallelDo(CLEANER, Avros.strings());
    File cso = tmpDir.getFile("cleanShakes");
    cleanShakes.write(To.textFile(cso.getAbsolutePath()));
    
    File wc = tmpDir.getFile("wordCounts");
    cleanShakes.parallelDo(SPLIT, Avros.strings()).count().write(To.textFile(wc.getAbsolutePath()));
    pipeline.done();
    
    File cleanFile = new File(cso, "part-m-00000");
    List<String> lines = Files.readLines(cleanFile, Charset.defaultCharset());
    assertEquals(LINES_IN_SHAKES, lines.size());
  }
}
