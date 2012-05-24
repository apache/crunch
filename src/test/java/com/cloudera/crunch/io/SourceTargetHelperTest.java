package com.cloudera.crunch.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class SourceTargetHelperTest {

	@Test
	public void testGetNonexistentPathSize() throws Exception {
		File tmp = File.createTempFile("pathsize", "");
		Path tmpPath = new Path(tmp.getAbsolutePath());
		tmp.delete();
		FileSystem fs = FileSystem.getLocal(new Configuration());
		assertEquals(-1L, SourceTargetHelper.getPathSize(fs, tmpPath));
	}

	@Test
	public void testGetNonExistentPathSize_NonExistantPath() throws IOException {
		FileSystem mockFs = new MockFileSystem();
		assertEquals(-1L, SourceTargetHelper.getPathSize(mockFs, new Path("does/not/exist")));
	}

	/**
	 * Mock FileSystem that returns null for {@link FileSystem#listStatus(Path)}.
	 */
	static class MockFileSystem extends LocalFileSystem {

		@Override
		public FileStatus[] listStatus(Path f) throws IOException {
			return null;
		}
	}
}
