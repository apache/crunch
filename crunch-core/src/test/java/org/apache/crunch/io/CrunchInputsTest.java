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

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CrunchInputsTest {

  private Job job;
  private FormatBundle formatBundle;

  @Before
  public void setUp() throws IOException {
    job = new Job();
    formatBundle = FormatBundle.forInput(TextInputFormat.class);
  }

  @Test
  public void testAddAndGetPaths() {
    Path inputPath = new Path("/tmp");
    CrunchInputs.addInputPath(job, inputPath, formatBundle, 0);

    assertEquals(
        ImmutableMap.of(formatBundle, ImmutableMap.of(0, ImmutableList.of(inputPath))),
        CrunchInputs.getFormatNodeMap(job));

  }

  @Test
  public void testAddAndGetPaths_GlobPath() {
    Path globPath = new Path("/tmp/file{1,2,3}.txt");
    CrunchInputs.addInputPath(job, globPath, formatBundle, 0);

    assertEquals(
        ImmutableMap.of(formatBundle, ImmutableMap.of(0, ImmutableList.of(globPath))),
        CrunchInputs.getFormatNodeMap(job));
  }

  @Test
  public void testAddAndGetPaths_PathUsingSeparators() {
    Path pathUsingRecordSeparators = new Path("/tmp/,;|.txt");
    CrunchInputs.addInputPath(job, pathUsingRecordSeparators, formatBundle, 0);

    assertEquals(
        ImmutableMap.of(formatBundle, ImmutableMap.of(0, ImmutableList.of(pathUsingRecordSeparators))),
        CrunchInputs.getFormatNodeMap(job));
  }

  @Test
  public void testAddAndGetPaths_FullyQualifiedPath() throws IOException {
    Path fullyQualifiedPath = new Path("/tmp").makeQualified(FileSystem.getLocal(new Configuration()));
    System.out.println("Fully qualified: " + fullyQualifiedPath);
    CrunchInputs.addInputPath(job, fullyQualifiedPath, formatBundle, 0);

    assertEquals(
        ImmutableMap.of(formatBundle, ImmutableMap.of(0, ImmutableList.of(fullyQualifiedPath))),
        CrunchInputs.getFormatNodeMap(job));

  }
}
