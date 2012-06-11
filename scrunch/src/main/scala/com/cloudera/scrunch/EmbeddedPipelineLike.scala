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

import com.cloudera.crunch.Source
import com.cloudera.crunch.TableSource
import com.cloudera.crunch.Target

trait EmbeddedPipelineLike { self: EmbeddedPipeline =>
  /**
   * Reads a source into a [[com.cloudera.scrunch.PCollection]]
   *
   * @param source The source to read from.
   * @tparam T The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[T](source: Source[T]): PCollection[T] = {
    pipeline.read(source)
  }

  /**
   * Reads a source into a [[com.cloudera.scrunch.PTable]]
   *
   * @param source The source to read from.
   * @tparam K The type of the keys being read.
   * @tparam V The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[K, V](source: TableSource[K, V]): PTable[K, V] = {
    pipeline.read(source)
  }

  /**
   * Reads a source into a [[com.cloudera.scrunch.PCollection]]
   *
   * @param source The source to read from.
   * @tparam T The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def load[T](source: Source[T]): PCollection[T] = {
    read(source)
  }

  /**
   * Reads a source into a [[com.cloudera.scrunch.PTable]]
   *
   * @param source The source to read from.
   * @tparam K The type of the keys being read.
   * @tparam V The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def load[K, V](source: TableSource[K, V]): PTable[K, V] = {
    read(source)
  }

  /**
   * Writes a parallel collection to a target.
   *
   * @param collection The collection to write.
   * @param target The destination target for this write.
   */
  def write(collection: PCollection[_], target: Target) {
    pipeline.write(collection, target)
  }

  /**
   * Writes a parallel table to a target.
   *
   * @param table The table to write.
   * @param target The destination target for this write.
   */
  def write(table: PTable[_, _], target: Target) {
    pipeline.write(table, target)
  }

  /**
   * Writes a parallel collection to a target.
   *
   * @param collection The collection to write.
   * @param target The destination target for this write.
   */
  def store(collection: PCollection[_], target: Target) {
    write(collection, target)
  }

  /**
   * Writes a parallel table to a target.
   *
   * @param table The table to write.
   * @param target The destination target for this write.
   */
  def store(table: PTable[_, _], target: Target) {
    write(table, target)
  }

  /**
   * Constructs and executes a series of MapReduce jobs in order
   * to write data to the output targets.
   */
  def run() {
    pipeline.run
  }

  /**
   * Run any remaining jobs required to generate outputs and then
   * clean up any intermediate data files that were created in
   * this run or previous calls to `run`.
   */
  def done() {
    pipeline.done
  }
}
