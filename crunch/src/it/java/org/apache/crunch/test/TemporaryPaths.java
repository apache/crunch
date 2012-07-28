package org.apache.crunch.test;

import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.hadoop.conf.Configuration;


/**
 * Utilities for working with {@link TemporaryPath}.
 */
public final class TemporaryPaths {

  /**
   * Static factory returning a {@link TemporaryPath} with adjusted
   * {@link Configuration} properties.
   */
  public static TemporaryPath create() {
    return new TemporaryPath(RuntimeParameters.TMP_DIR, "hadoop.tmp.dir");
  }

  private TemporaryPaths() {
    // nothing
  }
}
