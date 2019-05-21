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

import java.io.IOException;

import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import org.junit.Rule;
import org.junit.Test;

public class SourceTargetHelperTest {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testGetNonexistentPathSize() throws Exception {
    Path tmpPath = tmpDir.getRootPath();
    tmpDir.delete();
    FileSystem fs = FileSystem.getLocal(tmpDir.getDefaultConfiguration());
    assertEquals(-1L, SourceTargetHelper.getPathSize(fs, tmpPath));
  }

  @Test
  public void testGetNonExistentPathSize_NonExistantPath() throws IOException {
    FileSystem mockFs = new MockFileSystem();
    assertEquals(-1L, SourceTargetHelper.getPathSize(mockFs, new Path("does/not/exist")));
  }

  /**
   * Tests for proper recursive size calculation on a path containing a glob pattern.
   */
  @Test
  public void testGetPathSizeGlobPathRecursive() throws Exception {
    FileSystem fs = FileSystem.getLocal(tmpDir.getDefaultConfiguration());

    // Create a directory structure with 3 files spread across 2 top-level directories and one subdirectory:
    // foo1/file1
    // foo1/subdir/file2
    // foo2/file3
    Path foo1 = tmpDir.getPath("foo1");
    fs.mkdirs(foo1);
    createFile(fs, new Path(foo1, "file1"), 3);

    Path subDir = tmpDir.getPath("foo1/subdir");
    fs.mkdirs(subDir);
    createFile(fs, new Path(subDir, "file2"), 5);

    Path foo2 = tmpDir.getPath("foo2");
    fs.mkdirs(foo2);
    createFile(fs, new Path(foo2, "file3"), 11);

    // assert total size with glob pattern (3 + 5 + 11 = 19)
    assertEquals(19, SourceTargetHelper.getPathSize(fs, tmpDir.getPath("foo*")));
  }

  private static void createFile(FileSystem fs, Path path, int size) throws IOException {
    FSDataOutputStream outputStream = fs.create(path);
    for (int i = 0; i < size; i++) {
      outputStream.write(0);
    }
    outputStream.close();
  }

  /**
   * Mock FileSystem that returns null for {@link FileSystem#listStatus(Path)}.
   */
  private static class MockFileSystem extends LocalFileSystem {

    private static RawLocalFileSystem createConfiguredRawLocalFileSystem() {
      RawLocalFileSystem fs = new RawLocalFileSystem();
      fs.setConf(new Configuration(false));
      return fs;
    }

    private MockFileSystem() {
      super(createConfiguredRawLocalFileSystem());
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return null;
    }
  }
}
