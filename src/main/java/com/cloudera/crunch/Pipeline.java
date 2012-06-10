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
package com.cloudera.crunch;

import org.apache.hadoop.conf.Configuration;

/**
 * Manages the state of a pipeline execution.
 * 
 */
public interface Pipeline {
  
  /**
   * Set the {@code Configuration} to use with this pipeline.
   */
  void setConfiguration(Configuration conf);
  
  /**
   * Returns the name of this pipeline.
   * @return Name of the pipeline
   */
  String getName();
  
  /**
   * Returns the {@code Configuration} instance associated with this pipeline.
   */
  Configuration getConfiguration();
  
  /**
   * Converts the given {@code Source} into a {@code PCollection} that is
   * available to jobs run using this {@code Pipeline} instance.
   * 
   * @param source The source of data
   * @return A PCollection that references the given source
   */
  <T> PCollection<T> read(Source<T> source);

  /**
   * A version of the read method for {@code TableSource} instances that
   * map to {@code PTable}s.
   * @param tableSource The source of the data
   * @return A PTable that references the given source
   */
  <K, V> PTable<K, V> read(TableSource<K, V> tableSource);
  
  /**
   * Write the given collection to the given target on the next
   * pipeline run.
   * 
   * @param collection The collection
   * @param target The output target
   */
  void write(PCollection<?> collection, Target target);

  /**
   * Create the given PCollection and read the data it contains
   * into the returned Collection instance for client use.
   *
   * @param pcollection The PCollection to materialize
   * @return the data from the PCollection as a read-only Collection
   */
  <T> Iterable<T> materialize(PCollection<T> pcollection);
  
  /**
   * Constructs and executes a series of MapReduce jobs in order
   * to write data to the output targets.
   */
  PipelineResult run();

  /**
   * Run any remaining jobs required to generate outputs and then
   * clean up any intermediate data files that were created in
   * this run or previous calls to {@code run}.
   */
  PipelineResult done();

  /**
   * A convenience method for reading a text file.
   */
  PCollection<String> readTextFile(String pathName);

  /**
   * A convenience method for writing a text file.
   */
  <T> void writeTextFile(PCollection<T> collection, String pathName);
  
  /**
   * Turn on debug logging for jobs that are run from this pipeline.
   */
  void enableDebug();
}
