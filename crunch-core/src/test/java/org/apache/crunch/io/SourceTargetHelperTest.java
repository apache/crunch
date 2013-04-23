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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class SourceTargetHelperTest {

  @Test
  public void testGetNonexistentPathSize() throws Exception {
    File tmp = File.createTempFile("pathsize", "");
    Path tmpPath = new Path(tmp.getAbsolutePath());
    tmp.delete();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertEquals(-1L, SourceTargetHelper.getPathSize(fs, tmpPath));
  }

  @Test
  public void testGetNonExistentPathSize_NonExistantPath() throws IOException {
    FileSystem mockFs = new MockFileSystem();
    assertEquals(-1L, SourceTargetHelper.getPathSize(mockFs, new Path("does/not/exist")));
  }

  /**
   * Mock FileSystem that returns null for {@link FileSystem#listStatus(Path)}.
   */
  static class MockFileSystem extends LocalFileSystem {

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return null;
    }
  }
}
