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
package org.apache.crunch.lib;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class ShardIT {

  @Rule
  public TemporaryPath tempDir = new TemporaryPath("crunch.tmp.dir", "hadoop.tmp.dir");

  @Test
  public void testShard() throws Exception {
    File inDir = tempDir.getFile("in");
    FileUtils.writeLines(new File(inDir, "part1"), ImmutableList.of("part1", "part1"));
    FileUtils.writeLines(new File(inDir, "part2"), ImmutableList.of("part2"));
    Pipeline pipeline = new MRPipeline(ShardIT.class);
    PCollection<String> in = pipeline.read(From.textFile(inDir.getPath()));
    // We can only test on 1 shard here, as local MR does not support multiple reducers.
    PCollection<String> out = Shard.shard(in, 1);
    assertEquals(
        ImmutableMultiset.copyOf(out.materialize()),
        ImmutableMultiset.of("part1", "part1", "part2"));
  }
}
