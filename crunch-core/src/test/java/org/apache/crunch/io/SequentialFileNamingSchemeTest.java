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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SequentialFileNamingSchemeTest {

  // The partition id used for testing. This partition id should be ignored by
  // the SequentialFileNamingScheme.
  private static final int PARTITION_ID = 42;

  private SequentialFileNamingScheme namingScheme;
  private Configuration configuration;

  @Rule
  public TemporaryFolder tmpOutputDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    configuration = new Configuration();
    namingScheme = new SequentialFileNamingScheme();
  }

  @Test
  public void testGetMapOutputName_EmptyDirectory() throws IOException {
    assertEquals("part-m-00000",
        namingScheme.getMapOutputName(configuration, new Path(tmpOutputDir.getRoot().getAbsolutePath())));
  }

  @Test
  public void testGetMapOutputName_NonEmptyDirectory() throws IOException {
    File outputDirectory = tmpOutputDir.getRoot();

    new File(outputDirectory, "existing-1").createNewFile();
    new File(outputDirectory, "existing-2").createNewFile();

    assertEquals("part-m-00002",
        namingScheme.getMapOutputName(configuration, new Path(outputDirectory.getAbsolutePath())));
  }

  @Test
  public void testGetReduceOutputName_EmptyDirectory() throws IOException {
    assertEquals("part-r-00000", namingScheme.getReduceOutputName(configuration, new Path(tmpOutputDir.getRoot()
        .getAbsolutePath()), PARTITION_ID));
  }

  @Test
  public void testGetReduceOutputName_NonEmptyDirectory() throws IOException {
    File outputDirectory = tmpOutputDir.getRoot();

    new File(outputDirectory, "existing-1").createNewFile();
    new File(outputDirectory, "existing-2").createNewFile();

    assertEquals("part-r-00002",
        namingScheme.getReduceOutputName(configuration, new Path(outputDirectory.getAbsolutePath()), PARTITION_ID));
  }

}
