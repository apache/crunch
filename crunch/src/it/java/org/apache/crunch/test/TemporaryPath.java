package org.apache.crunch.test;

import java.io.File;
import java.io.IOException;

import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Creates a temporary directory for a test case and destroys it afterwards.
 * This provides a temporary directory like JUnit's {@link TemporaryFolder} but
 * geared towards Hadoop applications. Unlike {@link TemporaryFolder}, it
 * doesn't create any files or directories except for the root directory itself.
 */
public final class TemporaryPath extends ExternalResource {
  private TemporaryFolder tmp = new TemporaryFolder();

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
   * Copy a classpath resource to a {@link Path}
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

  public Configuration setTempLoc(Configuration config) throws IOException {
    config.set(RuntimeParameters.TMP_DIR, getRootFileName());
    config.set("hadoop.tmp.dir", getFileName("hadoop-tmp"));
    return config;
  }
}
