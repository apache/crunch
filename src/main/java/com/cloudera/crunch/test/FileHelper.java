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
