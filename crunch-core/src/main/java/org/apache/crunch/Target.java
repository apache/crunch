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
package org.apache.crunch;

import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * A {@code Target} represents the output destination of a Crunch {@code PCollection}
 * in the context of a Crunch job.
 */
public interface Target {

  /**
   * An enum to represent different options the client may specify
   * for handling the case where the output path, table, etc. referenced
   * by a {@code Target} already exists.
   */
  enum WriteMode {
    /**
     * Check to see if the output target already exists before running
     * the pipeline, and if it does, print an error and throw an exception.
     */
    DEFAULT,
    
    /**
     * Check to see if the output target already exists, and if it does,
     * delete it and overwrite it with the new output (if any).
     */
    OVERWRITE,

    /**
     * If the output target does not exist, create it. If it does exist,
     * add the output of this pipeline to the target. This was the
     * behavior in Crunch up to version 0.4.0.
     */
    APPEND,
    
    /**
     * If the output target exists and is newer than any of its source inputs, don't rewrite it,
     * just start the pipeline from here. Only works with {@code SourceTarget} instances.
     */
    CHECKPOINT
  }

  /**
   * Adds the given key-value pair to the {@code Configuration} instance that is used to write
   * this {@code Target}. Allows for multiple target outputs to re-use the same config keys with
   * different values when necessary.
   */
  Target outputConf(String key, String value);

  /**
   * Adds the {@code Configuration} of the given filesystem such that the target can write to it when the {@code
   * Pipeline} itself does not have that configuration.
   * </p>
   * Changing the filesystem after it is set is not supported and will result in {@link
   * IllegalStateException}
   *
   * @param fileSystem the filesystem
   * @return this Target
   * @throws IllegalStateException if the filesystem has already been set
   * @throws IllegalArgumentException if the target is pointing to a fully qualified Path in a different FileSystem
   */
  Target fileSystem(FileSystem fileSystem);

  /**
   * Returns the {@code FileSystem} for this target or null if no explicit filesystem {@link #fileSystem(FileSystem)
   * has been set}.
   */
  FileSystem getFileSystem();

  /**
   * Apply the given {@code WriteMode} to this {@code Target} instance.
   * 
   * @param writeMode The strategy for handling existing outputs
   * @param lastModifiedAt the time of the most recent modification to one of the source inputs for handling based
   *                       on the provided {@code writeMode}, or -1 if not relevant for the provided {@code writeMode}
   * @param conf The ever-useful {@code Configuration} instance
   * @return true if the target did exist
   */
  boolean handleExisting(WriteMode writeMode, long lastModifiedAt, Configuration conf);
  
  /**
   * Checks to see if this {@code Target} instance is compatible with the
   * given {@code PType}.
   * 
   * @param handler The {@link OutputHandler} that is managing the output for the job
   * @param ptype The {@code PType} to check
   * @return True if this Target can write data in the form of the given {@code PType},
   * false otherwise
   */
  boolean accept(OutputHandler handler, PType<?> ptype);

  /**
   * Returns the {@code Converter} to use for mapping from the output {@code PCollection}
   * into the output values expected by this instance.
   * 
   * @param ptype The {@code PType} of the data that is being written to this instance
   * @return A valid {@code Converter} for the output represented by this instance
   */
  Converter<?, ?, ?, ?> getConverter(PType<?> ptype);
  
  /**
   * Attempt to create the {@code SourceTarget} type that corresponds to this {@code Target}
   * for the given {@code PType}, if possible. If it is not possible, return {@code null}.
   * 
   * @param ptype The {@code PType} to use in constructing the {@code SourceTarget}
   * @return A new {@code SourceTarget} or null if such a {@code SourceTarget} does not exist
   */
  <T> SourceTarget<T> asSourceTarget(PType<T> ptype);
}
