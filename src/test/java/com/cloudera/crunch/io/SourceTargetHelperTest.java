package com.cloudera.crunch.io;

import static org.junit.Assert.fail;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class SourceTargetHelperTest {
  @Test
  public void testGetNonexistentPathSize() throws Exception {
	File tmp = File.createTempFile("pathsize", "");
	Path tmpPath = new Path(tmp.getAbsolutePath());
	tmp.delete();
	FileSystem fs = FileSystem.getLocal(new Configuration());
	try {
	  SourceTargetHelper.getPathSize(fs, tmpPath);
	  fail("Path defined");
	} catch (IllegalArgumentException e) {
	  // expected
	}
  }
}
