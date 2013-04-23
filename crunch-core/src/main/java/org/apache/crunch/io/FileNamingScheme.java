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
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates rules for naming output files. It is the responsibility of
 * implementors to avoid file name collisions.
 */
public interface FileNamingScheme {

  /**
   * Get the output file name for a map task. Note that the implementation is
   * responsible for avoiding naming collisions.
   * 
   * @param configuration The configuration of the job for which the map output
   *          is being written
   * @param outputDirectory The directory where the output will be written
   * @return The filename for the output of the map task
   * @throws IOException if an exception occurs while accessing the output file
   *           system
   */
  String getMapOutputName(Configuration configuration, Path outputDirectory) throws IOException;

  /**
   * Get the output file name for a reduce task. Note that the implementation is
   * responsible for avoiding naming collisions.
   * 
   * @param configuration The configuration of the job for which output is being
   *          written
   * @param outputDirectory The directory where the file will be written
   * @param partitionId The partition of the reduce task being output
   * @return The filename for the output of the reduce task
   * @throws IOException if an exception occurs while accessing output file
   *           system
   */
  String getReduceOutputName(Configuration configuration, Path outputDirectory, int partitionId) throws IOException;

}
