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
package org.apache.crunch.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Default {@link FileNamingScheme} that uses an incrementing sequence number in
 * order to generate unique file names.
 */
public class SequentialFileNamingScheme implements FileNamingScheme {

  @Override
  public String getMapOutputName(Configuration configuration, Path outputDirectory) throws IOException {
    return getSequentialFileName(configuration, outputDirectory, "m");
  }

  @Override
  public String getReduceOutputName(Configuration configuration, Path outputDirectory, int partitionId)
      throws IOException {
    return getSequentialFileName(configuration, outputDirectory, "r");
  }

  private String getSequentialFileName(Configuration configuration, Path outputDirectory, String jobTypeName)
      throws IOException {
    FileSystem fileSystem = outputDirectory.getFileSystem(configuration);
    int fileSequenceNumber = fileSystem.listStatus(outputDirectory).length;

    return String.format("part-%s-%05d", jobTypeName, fileSequenceNumber);
  }

}
