package com.cloudera.crunch.test;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

public class FileHelper {

  public static String createTempCopyOf(String fileResource) throws IOException {
	File tmpFile = File.createTempFile("tmp", "");
	tmpFile.deleteOnExit();
	Files.copy(newInputStreamSupplier(getResource(fileResource)), tmpFile);
	return tmpFile.getAbsolutePath();
  }
  
  public static File createOutputPath() throws IOException {
    File output = File.createTempFile("output", "");
    output.delete();
    return output;
  }
}
