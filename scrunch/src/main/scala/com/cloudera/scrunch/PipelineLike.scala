/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.scrunch

import org.apache.hadoop.conf.Configuration

import com.cloudera.crunch.{Pipeline => JPipeline}
import com.cloudera.crunch.Source
import com.cloudera.crunch.TableSource
import com.cloudera.crunch.Target

trait PipelineLike {
  def jpipeline: JPipeline

  /**
   * Gets the configuration object associated with this pipeline.
   */
  def getConfiguration(): Configuration = jpipeline.getConfiguration()

  /**
   * Reads a source into a [[com.cloudera.scrunch.PCollection]]
   *
   * @param source The source to read from.
   * @tparam T The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[T](source: Source[T]): PCollection[T] = new PCollection(jpipeline.read(source))

  /**
   * Reads a source into a [[com.cloudera.scrunch.PTable]]
   *
   * @param source The source to read from.
   * @tparam K The type of the keys being read.
   * @tparam V The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[K, V](source: TableSource[K, V]): PTable[K, V] = new PTable(jpipeline.read(source))

  /**
   * Writes a parallel collection to a target.
   *
   * @param collection The collection to write.
   * @param target The destination target for this write.
   */
  def write(collection: PCollection[_], target: Target): Unit = jpipeline.write(collection.native, target)

  /**
   * Writes a parallel table to a target.
   *
   * @param table The table to write.
   * @param target The destination target for this write.
   */
  def write(table: PTable[_, _], target: Target): Unit = jpipeline.write(table.native, target)

  /**
   * Constructs and executes a series of MapReduce jobs in order
   * to write data to the output targets.
   */
  def run(): Unit = jpipeline.run()

  /**
   * Run any remaining jobs required to generate outputs and then
   * clean up any intermediate data files that were created in
   * this run or previous calls to `run`.
   */
  def done(): Unit = jpipeline.done()

  /**
   * Turn on debug logging for jobs that are run from this pipeline.
   */
  def debug(): Unit = jpipeline.enableDebug()
}
