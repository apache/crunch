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

import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

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
   * 
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
   * @param source
   *          The source of data
   * @return A PCollection that references the given source
   */
  <T> PCollection<T> read(Source<T> source);

  /**
   * Converts the given {@code Source} into a {@code PCollection} that is
   * available to jobs run using this {@code Pipeline} instance.
   *
   * @param source The source of data
   * @param named A name for the returned PCollection
   * @return A PCollection that references the given source
   */
  <T> PCollection<T> read(Source<T> source, String named);

  /**
   * A version of the read method for {@code TableSource} instances that map to
   * {@code PTable}s.
   * 
   * @param tableSource
   *          The source of the data
   * @return A PTable that references the given source
   */
  <K, V> PTable<K, V> read(TableSource<K, V> tableSource);

 /**
   * A version of the read method for {@code TableSource} instances that map to
   * {@code PTable}s.
   *
   * @param tableSource The source of the data
   * @param named A name for the returned PTable
   * @return A PTable that references the given source
   */
  <K, V> PTable<K, V> read(TableSource<K, V> tableSource, String named);

  /**
   * Write the given collection to the given target on the next pipeline run. The
   * system will check to see if the target's location already exists using the
   * {@code WriteMode.DEFAULT} rule for the given {@code Target}.
   * 
   * @param collection
   *          The collection
   * @param target
   *          The output target
   */
  void write(PCollection<?> collection, Target target);

  /**
  * Write the contents of the {@code PCollection} to the given {@code Target},
  * using the storage format specified by the target and the given
  * {@code WriteMode} for cases where the referenced {@code Target}
  * already exists.
  *
  * @param collection
  *          The collection
  * @param target
  *          The target to write to
  * @param writeMode
  *          The strategy to use for handling existing outputs
  */
 void write(PCollection<?> collection, Target target,
     Target.WriteMode writeMode);

 /**
   * Create the given PCollection and read the data it contains into the
   * returned Collection instance for client use.
   * 
   * @param pcollection
   *          The PCollection to materialize
   * @return the data from the PCollection as a read-only Collection
   */
  <T> Iterable<T> materialize(PCollection<T> pcollection);

  /**
   * Caches the given PCollection so that it will be processed at most once
   * during pipeline execution.
   *
   * @param pcollection The PCollection to cache
   * @param options The options for how the cached data is stored
   */
  <T> void cache(PCollection<T> pcollection, CachingOptions options);

  /**
   * Creates an empty {@code PCollection} of the given {@code PType}.
   *
   * @param ptype The PType of the empty PCollection
   * @return A valid PCollection with no contents
   */
  <T> PCollection<T> emptyPCollection(PType<T> ptype);

  /**
   * Creates an empty {@code PTable} of the given {@code PTable Type}.
   *
   * @param ptype The PTableType of the empty PTable
   * @return A valid PTable with no contents
   */
  <K, V> PTable<K, V> emptyPTable(PTableType<K, V> ptype);

  /**
   * Creates a {@code PCollection} containing the values found in the given {@code Iterable}
   * using an implementation-specific distribution mechanism.
   *
   * @param contents The values the new PCollection will contain
   * @param ptype The PType of the PCollection
   * @return A PCollection that contains the given values
   */
  <T> PCollection<T> create(Iterable<T> contents, PType<T> ptype);

  /**
   * Creates a {@code PCollection} containing the values found in the given {@code Iterable}
   * using an implementation-specific distribution mechanism.
   *
   * @param contents The values the new PCollection will contain
   * @param ptype The PType of the PCollection
   * @param options Additional options, such as the name or desired parallelism of the PCollection
   * @return A PCollection that contains the given values
   */
  <T> PCollection<T> create(Iterable<T> contents, PType<T> ptype, CreateOptions options);

  /**
   * Creates a {@code PTable} containing the values found in the given {@code Iterable}
   * using an implementation-specific distribution mechanism.
   *
   * @param contents The values the new PTable will contain
   * @param ptype The PTableType of the PTable
   * @return A PTable that contains the given values
   */
  <K, V> PTable<K, V> create(Iterable<Pair<K, V>> contents, PTableType<K, V> ptype);

  /**
   * Creates a {@code PTable} containing the values found in the given {@code Iterable}
   * using an implementation-specific distribution mechanism.
   *
   * @param contents The values the new PTable will contain
   * @param ptype The PTableType of the PTable
   * @param options Additional options, such as the name or desired parallelism of the PTable
   * @return A PTable that contains the given values
   */
  <K, V> PTable<K, V> create(Iterable<Pair<K, V>> contents, PTableType<K, V> ptype, CreateOptions options);

  <S> PCollection<S> union(List<PCollection<S>> collections);

  <K, V> PTable<K, V> unionTables(List<PTable<K, V>> tables);

  /**
   * Executes the given {@code PipelineCallable} on the client after the {@code Targets}
   * that the PipelineCallable depends on (if any) have been created by other pipeline
   * processing steps.
   *
   * @param pipelineCallable The sequential logic to execute
   * @param <Output> The return type of the PipelineCallable
   * @return The result of executing the PipelineCallable
   */
  <Output> Output sequentialDo(PipelineCallable<Output> pipelineCallable);

  /**
   * Constructs and executes a series of MapReduce jobs in order to write data
   * to the output targets.
   */
  PipelineResult run();

  /**
   * Constructs and starts a series of MapReduce jobs in order ot write data to
   * the output targets, but returns a {@code ListenableFuture} to allow clients to control
   * job execution.
   * @return
   */
  PipelineExecution runAsync();
  
  /**
   * Run any remaining jobs required to generate outputs and then clean up any
   * intermediate data files that were created in this run or previous calls to
   * {@code run}.
   */
  PipelineResult done();

  /**
  * Cleans up any artifacts created as a result of {@link #run() running} the pipeline.
  * @param force forces the cleanup even if all targets of the pipeline have not been completed.
  */
  void cleanup(boolean force);

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
