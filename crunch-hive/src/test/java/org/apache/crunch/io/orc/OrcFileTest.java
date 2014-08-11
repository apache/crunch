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
package org.apache.crunch.io.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

import com.google.common.io.Files;

public class OrcFileTest {

  protected transient Configuration conf;
  protected transient FileSystem fs;
  protected transient Path tempPath;
  
  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    tempPath = new Path(Files.createTempDir().getAbsolutePath());
    fs = tempPath.getFileSystem(conf);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(tempPath, true);
  }
  
}
