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

import static org.apache.crunch.types.writable.Writables.*;
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.text.TextFileTableSource;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 *
 */
public class TextFileTableIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test
  public void testTextFileTable() throws Exception {
    String urlsFile = tmpDir.copyResourceFileName("urls.txt");
    Pipeline pipeline = new MRPipeline(TextFileTableIT.class, tmpDir.getDefaultConfiguration());
    PTable<String, String> urls = pipeline.read(
        new TextFileTableSource<String, String>(urlsFile, tableOf(strings(), strings())));
    Set<Pair<String, Long>> cnts = ImmutableSet.copyOf(urls.keys().count().materialize());
    assertEquals(ImmutableSet.of(Pair.of("www.A.com", 4L), Pair.of("www.B.com", 2L),
        Pair.of("www.C.com", 1L), Pair.of("www.D.com", 1L), Pair.of("www.E.com", 1L),
        Pair.of("www.F.com", 2L)), cnts);
  }
}
