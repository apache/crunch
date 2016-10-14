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
package org.apache.crunch.util;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides functions for working with Hadoop's distributed cache. These
 * include:
 * <ul>
 * <li>
 * Functions for working with a job-specific distributed cache of objects, like
 * the serialized runtime nodes in a MapReduce.</li>
 * <li>
 * Functions for adding library jars to the distributed cache, which will be
 * added to the classpath of MapReduce tasks.</li>
 * </ul>
 */
public class DistCache {

  /**
   * Configuration key for setting the replication factor for files distributed using the Crunch
   * DistCache helper class. This can be used to scale read access for files used by the Crunch
   * framework.
   */
  public static final String DIST_CACHE_REPLICATION = "crunch.distcache.replication";

  // Configuration key holding the paths of jars to export to the distributed
  // cache.
  private static final String TMPJARS_KEY = "tmpjars";

  public static void write(Configuration conf, Path path, Object value) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    short replication = (short) conf.getInt(DIST_CACHE_REPLICATION, fs.getDefaultReplication(path));
    ObjectOutputStream oos = new ObjectOutputStream(fs.create(path, replication));
    oos.writeObject(value);
    oos.close();

    DistributedCache.addCacheFile(path.toUri(), conf);
  }

  public static Object read(Configuration conf, Path requestedFile) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);

    Path cachedPath = null;

    try {
      cachedPath = getPathToCacheFile(requestedFile, conf);
    } catch (CrunchRuntimeException cre) {
      throw new IOException("Can not determine cached location for " + requestedFile.toString(), cre);
    }

    if(cachedPath == null || !localFs.exists(cachedPath)) {
      throw new IOException("Expected file with path: " + requestedFile.toString() + " to be cached");
    }

    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(localFs.open(cachedPath));
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new CrunchRuntimeException(e);
    } finally {
      if (ois != null) {
        ois.close();
      }
    }
  }

  public static void addCacheFile(Path path, Configuration conf) {
    DistributedCache.addCacheFile(path.toUri(), conf);
  }
  
  public static Path getPathToCacheFile(Path path, Configuration conf) {
    try {
      for (Path localPath : DistributedCache.getLocalCacheFiles(conf)) {
        if (localPath.toString().endsWith(path.getName())) {
          return localPath.makeQualified(FileSystem.getLocal(conf));
        }
      }
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
    return null;
  }
  
  /**
   * Adds the specified jar to the distributed cache of jobs using the provided
   * configuration. The jar will be placed on the classpath of tasks run by the
   * job.
   * 
   * @param conf
   *          The configuration used to add the jar to the distributed cache.
   * @param jarFile
   *          The jar file to add to the distributed cache.
   * @throws IOException
   *           If the jar file does not exist or there is a problem accessing
   *           the file.
   */
  public static void addJarToDistributedCache(Configuration conf, File jarFile) throws IOException {
    if (!jarFile.exists()) {
      throw new IOException("Jar file: " + jarFile.getCanonicalPath() + " does not exist.");
    }
    if (!jarFile.getName().endsWith(".jar")) {
      throw new IllegalArgumentException("File: " + jarFile.getCanonicalPath() + " is not a .jar " + "file.");
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
   * Adds the jar at the specified path to the distributed cache of jobs using
   * the provided configuration. The jar will be placed on the classpath of
   * tasks run by the job.
   * 
   * @param conf
   *          The configuration used to add the jar to the distributed cache.
   * @param jarFile
   *          The path to the jar file to add to the distributed cache.
   * @throws IOException
   *           If the jar file does not exist or there is a problem accessing
   *           the file.
   */
  public static void addJarToDistributedCache(Configuration conf, String jarFile) throws IOException {
    addJarToDistributedCache(conf, new File(jarFile));
  }

  /**
   * Finds the path to a jar that contains the class provided, if any. There is
   * no guarantee that the jar returned will be the first on the classpath to
   * contain the file. This method is basically lifted out of Hadoop's
   * {@link org.apache.hadoop.mapred.JobConf} class.
   * 
   * @param jarClass
   *          The class the jar file should contain.
   * @return The path to a jar file that contains the class, or
   *         <code>null</code> if no such jar exists.
   * @throws IOException
   *           If there is a problem searching for the jar file.
   */
  public static String findContainingJar(Class<?> jarClass) throws IOException {
    ClassLoader loader = jarClass.getClassLoader();
    String classFile = jarClass.getName().replaceAll("\\.", "/") + ".class";
    for (Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }
    return null;
  }

  /**
   * Adds all jars under the specified directory to the distributed cache of
   * jobs using the provided configuration. The jars will be placed on the
   * classpath of tasks run by the job. This method does not descend into
   * subdirectories when adding jars.
   * 
   * @param conf
   *          The configuration used to add jars to the distributed cache.
   * @param jarDirectory
   *          A directory containing jar files to add to the distributed cache.
   * @throws IOException
   *           If the directory does not exist or there is a problem accessing
   *           the directory.
   */
  public static void addJarDirToDistributedCache(Configuration conf, File jarDirectory) throws IOException {
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
   * Adds all jars under the directory at the specified path to the distributed
   * cache of jobs using the provided configuration. The jars will be placed on
   * the classpath of the tasks run by the job. This method does not descend
   * into subdirectories when adding jars.
   * 
   * @param conf
   *          The configuration used to add jars to the distributed cache.
   * @param jarDirectory
   *          The path to a directory containing jar files to add to the
   *          distributed cache.
   * @throws IOException
   *           If the directory does not exist or there is a problem accessing
   *           the directory.
   */
  public static void addJarDirToDistributedCache(Configuration conf, String jarDirectory) throws IOException {
    addJarDirToDistributedCache(conf, new File(jarDirectory));
  }
}
