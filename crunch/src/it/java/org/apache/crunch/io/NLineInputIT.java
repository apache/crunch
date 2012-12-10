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
package org.apache.crunch.io;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.text.NLineFileSource;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

public class NLineInputIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test
  public void testNLine() throws Exception {
    String urlsInputPath = tmpDir.copyResourceFileName("urls.txt");
    Pipeline pipeline = new MRPipeline(NLineInputIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> urls = pipeline.read(new NLineFileSource<String>(urlsInputPath,
        Writables.strings(), 2));
    assertEquals(new Integer(2),
        urls.parallelDo(new LineCountFn(), Avros.ints()).max().getValue());
  }
  
  private static class LineCountFn extends DoFn<String, Integer> {

    private int lineCount = 0;
    
    @Override
    public void initialize() {
      this.lineCount = 0;
    }
    
    @Override
    public void process(String input, Emitter<Integer> emitter) {
      lineCount++;
    }
    
    @Override
    public void cleanup(Emitter<Integer> emitter) {
      emitter.emit(lineCount);
    }
  }
}
