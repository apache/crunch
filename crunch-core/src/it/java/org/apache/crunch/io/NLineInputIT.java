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

import com.google.common.collect.ImmutableList;
import org.apache.crunch.CreateOptions;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class NLineInputIT {

  private static List<String> URLS = ImmutableList.of(
  "www.A.com       www.B.com",
  "www.A.com       www.C.com",
  "www.A.com       www.D.com",
  "www.A.com       www.E.com",
  "www.B.com       www.D.com",
  "www.B.com       www.E.com",
  "www.C.com       www.D.com",
  "www.D.com       www.B.com",
  "www.E.com       www.A.com",
  "www.F.com       www.B.com",
  "www.F.com       www.C.com");

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testNLine() throws Exception {
    Configuration conf = new Configuration(tmpDir.getDefaultConfiguration());
    conf.setInt("io.sort.mb", 10);
    Pipeline pipeline = new MRPipeline(NLineInputIT.class, conf);
    PCollection<String> urls = pipeline.create(URLS, Writables.strings(),
        CreateOptions.parallelism(6));
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
