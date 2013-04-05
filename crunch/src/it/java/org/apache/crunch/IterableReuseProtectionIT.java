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
import java.util.Collections;
import java.util.List;

import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Verify that calling the iterator method on a Reducer-based Iterable 
 * is forcefully disallowed.
 */
public class IterableReuseProtectionIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  
  public void checkIteratorReuse(Pipeline pipeline) throws IOException {
    Iterable<String> values = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .by(IdentityFn.<String>getInstance(), Writables.strings())
        .groupByKey()
        .combineValues(new TestIterableReuseFn())
        .values().materialize();
    
    List<String> valueList = Lists.newArrayList(values);
    Collections.sort(valueList);
    assertEquals(Lists.newArrayList("a", "b", "c", "e"), valueList);
  }
  
  @Test
  public void testIteratorReuse_MRPipeline() throws IOException {
    checkIteratorReuse(new MRPipeline(IterableReuseProtectionIT.class, tmpDir.getDefaultConfiguration()));
  }
  
  @Test
  public void testIteratorReuse_InMemoryPipeline() throws IOException {
    checkIteratorReuse(MemPipeline.getInstance());
  }
  
  static class TestIterableReuseFn extends CombineFn<String, String> {

    @Override
    public void process(Pair<String, Iterable<String>> input, Emitter<Pair<String, String>> emitter) {
      StringBuilder combinedBuilder = new StringBuilder();
      for (String v : input.second()) {
        combinedBuilder.append(v);
      }
      
      try {
        input.second().iterator();
        throw new RuntimeException("Second call to iterator should throw an exception");
      } catch (IllegalStateException e) {
        // Expected situation
      }
      emitter.emit(Pair.of(input.first(), combinedBuilder.toString()));
    }
    
  }
  
}
