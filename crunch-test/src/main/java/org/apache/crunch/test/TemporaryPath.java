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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Creates a temporary directory for a test case and destroys it afterwards.
 *
 * This provides a temporary directory like JUnit's {@link TemporaryFolder} but
 * geared towards Hadoop applications. Unlike {@link TemporaryFolder}, it
 * doesn't create any files or directories except for the root directory itself.
 *
 * Also, {@link #getDefaultConfiguration()} provides you with a configuration that
 * overrides path properties with temporary directories. You have to specify these
 * properties via the constructor.
 */
public final class TemporaryPath extends ExternalResource {
  private final TemporaryFolder tmp = new TemporaryFolder();
  private final Set<String> confKeys;

  /**
   * Construct {@link TemporaryPath}.
   * @param confKeys {@link Configuration} keys containing directories to override
   */
  public TemporaryPath(String... confKeys) {
    if (confKeys != null) {
      this.confKeys = ImmutableSet.copyOf(confKeys);
    } else {
      this.confKeys = ImmutableSet.of();
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return tmp.apply(base, description);
  }

  /**
   * Get the root directory which will be deleted automatically.
   */
  public File getRootFile() {
    return tmp.getRoot();
  }

  /**
   * Get the root directory as a {@link Path}.
   */
  public Path getRootPath() {
    return toPath(tmp.getRoot());
  }

  /**
   * Get the root directory as an absolute file name.
   */
  public String getRootFileName() {
    return tmp.getRoot().getAbsolutePath();
  }

  /**
   * Get a {@link File} below the temporary directory.
   */
  public File getFile(String fileName) {
    return new File(getRootFile(), fileName);
  }

  /**
   * Get a {@link Path} below the temporary directory.
   */
  public Path getPath(String fileName) {
    return toPath(getFile(fileName));
  }

  /**
   * Get an absolute file name below the temporary directory.
   */
  public String getFileName(String fileName) {
    return getFile(fileName).getAbsolutePath();
  }

  /**
   * Copy a classpath resource to {@link File}.
   */
  public File copyResourceFile(String resourceName) throws IOException {
    File dest = new File(tmp.getRoot(), resourceName);
    copy(resourceName, dest);
    return dest;
  }

  /**
   * Copy a classpath resource to a {@link Path}.
   */
  public Path copyResourcePath(String resourceName) throws IOException {
    return toPath(copyResourceFile(resourceName));
  }

  /**
   * Copy a classpath resource returning its absolute file name.
   */
  public String copyResourceFileName(String resourceName) throws IOException {
    return copyResourceFile(resourceName).getAbsolutePath();
  }

  private void copy(String resourceName, File dest) throws IOException {
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource(resourceName)), dest);
  }

  private static Path toPath(File file) {
    return new Path(file.getAbsolutePath());
  }

  public Configuration getDefaultConfiguration() {
    return overridePathProperties(new Configuration());
  }

  /**
   * Set all keys specified in the constructor to temporary directories.
   */
  public Configuration overridePathProperties(Configuration config) {
    for (String name : confKeys) {
      config.set(name, getFileName("tmp-" + name));
    }
    return config;
  }
}
