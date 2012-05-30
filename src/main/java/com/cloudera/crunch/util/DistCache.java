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

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;

/**
 * Provides functions for working with Hadoop's distributed cache. These include:
 * <ul>
 *   <li>
 *     Functions for working with a job-specific distributed cache of objects, like the
 *     serialized runtime nodes in a MapReduce.
 *   </li>
 *   <li>
 *     Functions for adding library jars to the distributed cache, which will be added to the
 *     classpath of MapReduce tasks.
 *   </li>
 * </ul>
 */
public class DistCache {

  // Configuration key holding the paths of jars to export to the distributed cache.
  private static final String TMPJARS_KEY = "tmpjars";

  public static void write(Configuration conf, Path path, Object value) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(FileSystem.get(conf).create(path));
    oos.writeObject(value);
    oos.close();

    DistributedCache.addCacheFile(path.toUri(), conf);
  }

  public static Object read(Configuration conf, Path path) throws IOException {
    URI target = null;
    for (URI uri : DistributedCache.getCacheFiles(conf)) {
      if (uri.toString().equals(path.toString())) {
        target = uri;
        break;
      }
    }
    Object value = null;
    if (target != null) {
      Path targetPath = new Path(target.toString());
      ObjectInputStream ois = new ObjectInputStream(targetPath.getFileSystem(conf).open(targetPath));
      try {
        value = ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
      ois.close();
    }
    return value;
  }

  /**
   * Adds the specified jar to the distributed cache of jobs using the provided configuration. The
   * jar will be placed on the classpath of tasks run by the job.
   *
   * @param conf The configuration used to add the jar to the distributed cache.
   * @param jarFile The jar file to add to the distributed cache.
   * @throws IOException If the jar file does not exist or there is a problem accessing the file.
   */
  public static void addJarToDistributedCache(Configuration conf, File jarFile) throws IOException {
    if (!jarFile.exists()) {
      throw new IOException("Jar file: " + jarFile.getCanonicalPath() + " does not exist.");
    }
    if (!jarFile.getName().endsWith(".jar")) {
      throw new IllegalArgumentException("File: " + jarFile.getCanonicalPath() + " is not a .jar "
          + "file.");
    }
    // Get a qualified path for the jar.
    FileSystem fileSystem = FileSystem.getLocal(conf);
    Path jarPath = new Path(jarFile.getCanonicalPath());
    String qualifiedPath = jarPath.makeQualified(fileSystem).toString();
    // Add the jar to the configuration variable.
    String jarConfiguration = conf.get(TMPJARS_KEY, "");
    if (!jarConfiguration.isEmpty()) {
      jarConfiguration += ",";
    }
    jarConfiguration += qualifiedPath;
    conf.set(TMPJARS_KEY, jarConfiguration);
  }

  /**
   * Adds the jar at the specified path to the distributed cache of jobs using the provided
   * configuration. The jar will be placed on the classpath of tasks run by the job.
   *
   * @param conf The configuration used to add the jar to the distributed cache.
   * @param jarFile The path to the jar file to add to the distributed cache.
   * @throws IOException If the jar file does not exist or there is a problem accessing the file.
   */
  public static void addJarToDistributedCache(Configuration conf, String jarFile)
      throws IOException {
    addJarToDistributedCache(conf, new File(jarFile));
  }

  /**
   * Adds all jars under the specified directory to the distributed cache of jobs using the
   * provided configuration. The jars will be placed on the classpath of tasks run by the job.
   * This method does not descend into subdirectories when adding jars.
   *
   * @param conf The configuration used to add jars to the distributed cache.
   * @param jarDirectory A directory containing jar files to add to the distributed cache.
   * @throws IOException If the directory does not exist or there is a problem accessing the
   *     directory.
   */
  public static void addJarDirToDistributedCache(Configuration conf, File jarDirectory)
      throws IOException {
    if (!jarDirectory.exists() || !jarDirectory.isDirectory()) {
      throw new IOException("Jar directory: " + jarDirectory.getCanonicalPath() + " does not "
        + "exist or is not a directory.");
    }
    for (File file : jarDirectory.listFiles()) {
      if (!file.isDirectory() && file.getName().endsWith(".jar")) {
        addJarToDistributedCache(conf, file);
      }
    }
  }

  /**
   * Adds all jars under the directory at the specified path to the distributed cache of jobs
   * using the provided configuration.  The jars will be placed on the classpath of the tasks
   * run by the job. This method does not descend into subdirectories when adding jars.
   *
   * @param conf The configuration used to add jars to the distributed cache.
   * @param jarDirectory The path to a directory containing jar files to add to the distributed
   *     cache.
   * @throws IOException If the directory does not exist or there is a problem accessing the
   *     directory.
   */
  public static void addJarDirToDistributedCache(Configuration conf, String jarDirectory)
      throws IOException {
    addJarDirToDistributedCache(conf, new File(jarDirectory));
  }
}
