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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.crunch.io.text.TextFileReaderFactory;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CompositePathIterableIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testCreate_FilePresent() throws IOException {
    String inputFilePath = tmpDir.copyResourceFileName("set1.txt");
    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);

    Iterable<String> iterable = CompositePathIterable.create(local, new Path(inputFilePath),
        new TextFileReaderFactory<String>(Writables.strings()));

    assertEquals(Lists.newArrayList("b", "c", "a", "e"), Lists.newArrayList(iterable));

  }

  @Test
  public void testCreate_DirectoryPresentButNoFiles() throws IOException {
    Path emptyInputDir = tmpDir.getRootPath();

    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);

    Iterable<String> iterable = CompositePathIterable.create(local, emptyInputDir,
        new TextFileReaderFactory<String>(Writables.strings()));

    assertTrue(Lists.newArrayList(iterable).isEmpty());
  }

  @Test(expected = IOException.class)
  public void testCreate_DirectoryNotPresent() throws IOException {
    File nonExistentDir = tmpDir.getFile("not-there");

    // Sanity check
    assertFalse(nonExistentDir.exists());

    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);

    CompositePathIterable.create(local, new Path(nonExistentDir.getAbsolutePath()), new TextFileReaderFactory<String>(
        Writables.strings()));
  }

  @Test
  public void testCreate_HiddenFiles() throws IOException {
    File file = tmpDir.copyResourceFile("set1.txt");
    assertTrue(file.renameTo(new File(tmpDir.getRootFile(), "_set1.txt")));

    file = tmpDir.copyResourceFile("set1.txt");
    assertTrue(file.renameTo(new File(tmpDir.getRootFile(), ".set1.txt")));

    Path emptyInputDir = tmpDir.getRootPath();

    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);

    Iterable<String> iterable = CompositePathIterable.create(local, emptyInputDir,
        new TextFileReaderFactory<String>(Writables.strings()));

    assertTrue(Lists.newArrayList(iterable).isEmpty());
  }

}
