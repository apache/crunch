/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.lib;

import java.io.IOException;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;

public class DistinctIT extends CrunchTestSupport {

  @Test
  public void testDistinct() throws IOException {
    Pipeline p = new MRPipeline(DistinctIT.class, tempDir.getDefaultConfiguration());
    Path inputPath = tempDir.copyResourcePath("list.txt");
    PCollection<String> in = p.read(From.textFile(inputPath));

    PCollection<String> distinct = Distinct.distinct(in);

    assertEquals(Lists.newArrayList("a", "b", "c", "d"), Lists.newArrayList(distinct.materialize()));
  }

  @Test
  public void testDistinctWithExplicitNumReducers() throws IOException {
    Pipeline p = new MRPipeline(DistinctIT.class, tempDir.getDefaultConfiguration());
    Path inputPath = tempDir.copyResourcePath("list.txt");
    PCollection<String> in = p.read(From.textFile(inputPath));

    PCollection<String> distinct = Distinct.distinct(in, 50, 1);

    assertEquals(Lists.newArrayList("a", "b", "c", "d"), Lists.newArrayList(distinct.materialize()));
  }

}
