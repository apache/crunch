/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DistCacheTest {

  // A temporary folder used to hold files created for the test.
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  // A configuration and lists of paths to use in tests.
  private Configuration testConf;
  private String[] testFilePaths;
  private String[] testFileQualifiedPaths;

  /**
   * Setup resources for tests.  These include:
   * 1. A Hadoop configuration.
   * 2. A directory of temporary files that includes 3 .jar files and 1 other file.
   * 3. Arrays containing the canonical paths and qualified paths to the test files.
   */
  @Before
  public void setup() throws IOException {
    // Create a configuration for tests.
    testConf = new Configuration();

    // Create the test files and add their paths to the list of test file paths.
    testFilePaths = new String[3];
    testFilePaths[0] = testFolder.newFile("jar1.jar").getCanonicalPath();
    testFilePaths[1] = testFolder.newFile("jar2.jar").getCanonicalPath();
    testFilePaths[2] = testFolder.newFile("jar3.jar").getCanonicalPath();
    testFolder.newFile("notJar.other");

    // Populate a list of qualified paths from the test file paths.
    testFileQualifiedPaths = new String[3];
    for (int i = 0; i < testFilePaths.length; i++) {
      testFileQualifiedPaths[i] = "file:" + testFilePaths[i];
    }
  }

  /**
   * Tests adding jars one-by-one to a job's configuration.
   *
   * @throws IOException If there is a problem adding the jars.
   */
  @Test
  public void testAddJar() throws IOException {
    // Add each valid jar path to the distributed cache configuration, and verify each was
    // added correctly in turn.
    for (int i = 0; i < testFilePaths.length; i++) {
      DistCache.addJarToDistributedCache(testConf, testFilePaths[i]);
      assertEquals("tmpjars configuration var does not contain expected value.",
          StringUtils.join(testFileQualifiedPaths, ",", 0, i + 1), testConf.get("tmpjars"));
    }
  }

  /**
   * Tests that attempting to add the path to a jar that does not exist to the configuration
   * throws an exception.
   *
   * @throws IOException If the added jar path does not exist. This exception is expected.
   */
  @Test(expected = IOException.class)
  public void testAddJarThatDoesntExist() throws IOException {
    DistCache.addJarToDistributedCache(testConf, "/garbage/doesntexist.jar");
  }

  /**
   * Tests that adding a directory of jars to the configuration works as expected. .jar files
   * under the added directory should be added to the configuration,
   * and all other files should be skipped.
   *
   * @throws IOException If there is a problem adding the jar directory to the configuration.
   */
  @Test
  public void testAddJarDirectory() throws IOException {
    DistCache.addJarDirToDistributedCache(testConf, testFolder.getRoot().getCanonicalPath());
    // Throw the added jar paths in a set to detect duplicates.
    String[] splitJarPaths = StringUtils.split(testConf.get("tmpjars"), ",");
    Set<String> addedJarPaths = new HashSet<String>();
    for (String path: splitJarPaths) {
      addedJarPaths.add(path);
    }
    assertEquals("Incorrect number of jar paths added.", testFilePaths.length,
        addedJarPaths.size());

    // Ensure all expected paths were added.
    for (int i = 0; i < testFileQualifiedPaths.length; i++) {
      assertTrue("Expected jar path missing from jar paths added to tmpjars: " +
          testFileQualifiedPaths[i], addedJarPaths.contains(testFileQualifiedPaths[i]));
    }
  }

  /**
   * Tests that adding a jar directory that does not exist to the configuration throws an
   * exception.
   *
   * @throws IOException If the added jar directory does not exist. This exception is expected.
   */
  @Test(expected = IOException.class)
  public void testAddJarDirectoryThatDoesntExist() throws IOException {
    DistCache.addJarDirToDistributedCache(testConf, "/garbage/doesntexist");
  }

  /**
   * Tests that adding a jar directory that is not a directory to the configuration throws an
   * exception.
   *
   * @throws IOException If the added jar directory is not a directory. This exception is expected.
   */
  @Test(expected = IOException.class)
  public void testAddJarDirectoryNotDirectory() throws IOException {
    DistCache.addJarDirToDistributedCache(testConf, testFilePaths[0]);
  }
}
