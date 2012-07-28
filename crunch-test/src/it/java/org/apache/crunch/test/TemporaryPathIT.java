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
package org.apache.crunch.test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Rule;
import org.junit.Test;


public class TemporaryPathIT {
  @Rule
  public TemporaryPath tmpDir = new TemporaryPath("foo.tmp", "bar.tmp");

  @Test
  public void testRoot() throws IOException {
    assertThat(tmpDir.getRootFile().exists(), is(true));
    assertThat(new File(tmpDir.getRootFileName()).exists(), is(true));
    assertThat(getFs().exists(tmpDir.getRootPath()), is(true));
  }

  @Test
  public void testFile() throws IOException {
    assertThat(tmpDir.getFile("foo").getParentFile(), is(tmpDir.getRootFile()));
    assertThat(tmpDir.getFile("foo").getName(), is("foo"));
    assertThat(tmpDir.getFile("foo").exists(), is(false));
  }

  @Test
  public void testPath() throws IOException {
    assertThat(tmpDir.getPath("foo").getParent(), is(tmpDir.getRootPath()));
    assertThat(tmpDir.getPath("foo").getName(), is("foo"));
    assertThat(getFs().exists(tmpDir.getPath("foo")), is(false));
  }

  @Test
  public void testFileName() {
    assertThat(new File(tmpDir.getRootFileName()), is(tmpDir.getRootFile()));
    assertThat(new File(tmpDir.getFileName("foo").toString()), is(tmpDir.getFile("foo")));
  }

  @Test
  public void testCopyResource() throws IOException {
    File dest = tmpDir.getFile("data.txt");
    assertThat(dest.exists(), is(false));

    tmpDir.copyResourceFile("data.txt");
    assertThat(dest.exists(), is(true));
  }

  @Test
  public void testGetDefaultConfiguration() {
    Configuration conf = tmpDir.getDefaultConfiguration();
    String fooDir = conf.get("foo.tmp");
    String barDir = conf.get("bar.tmp");

    assertThat(fooDir, startsWith(tmpDir.getRootFileName()));
    assertThat(barDir, startsWith(tmpDir.getRootFileName()));
    assertThat(fooDir, is(not(barDir)));
  }

  @Test
  public void testOverridePathProperties() {
    Configuration conf = new Configuration();
    conf.set("foo.tmp", "whatever");
    conf.set("other.dir", "/my/dir");

    tmpDir.overridePathProperties(conf);

    assertThat(conf.get("foo.tmp"), startsWith(tmpDir.getRootFileName()));
    assertThat(conf.get("other.dir"), is("/my/dir"));
  }

  private LocalFileSystem getFs() throws IOException {
    return FileSystem.getLocal(new Configuration());
  }
}
