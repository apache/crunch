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
package com.cloudera.scrunch

import org.apache.hadoop.conf.Configuration

import com.cloudera.crunch.{PCollection => JCollection, Pipeline => JPipeline}
import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.crunch.impl.mem.MemPipeline
import com.cloudera.crunch.impl.mr.MRPipeline
import com.cloudera.scrunch.Conversions._

/**
 * Manages the state of a pipeline execution.
 */
class Pipeline[R: ClassManifest](val conf: Configuration = new Configuration(), memory: Boolean = false) {
  import Pipeline._

  /**
   * Internal representation of this pipeline, a Crunch pipeline.
   */
  val jpipeline = if (memory) { MemPipeline.getInstance() } else { new MRPipeline(classManifest[R].erasure, conf) }

  /**
   * Gets the configuration object associated with this pipeline.
   */
  def getConfiguration = {
    jpipeline.getConfiguration()
  }

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

  /**
   * A convenience method for reading a text file.
   *
   * @param pathName Path to desired text file.
   * @return A PCollection containing the lines in the specified file.
   */
  def readTextFile(pathName: String) = new PCollection[String](jpipeline.readTextFile(pathName))

  /**
   * A convenience method for writing a text file.
   *
   * @param pcollect A PCollection to write to text.
   * @param pathName Path to desired output text file.
   */
  def writeTextFile[T](pcollect: PCollection[T], pathName: String) {
    jpipeline.writeTextFile(pcollect.native, pathName)
  }
}

/**
 * Companion object for [[com.cloudera.scrunch.Pipeline]].
 */
object Pipeline {
  /**
   * PWriters are used to perform the actual writing of PCollection and friends
   * to targets.
   *
   * @tparam C The type of the collection being written by this PWriter.
   */
  trait PWriter[C] {
    def write(collection: C, target: Target, pipeline: Pipeline[_]): Unit
  }

  /**
   * Companion object.
   */
  object PWriter {
    /**
     * Creates a PWriter that writes PCollections by delegating to the crunch pipeline.
     */
    implicit def PCollectionWriter[T] = new PWriter[PCollection[T]]() {
      def write(collection: PCollection[T], target: Target, pipeline: Pipeline[_]) {
        pipeline.jpipeline.write(collection.native, target)
      }
    }

    /**
     * Creates a PWriter that writes PTables by delegating to the crunch pipeline.
     */
    implicit def PTableWriter[K, V] = new PWriter[PTable[K, V]]() {
      def write(collection: PTable[K, V], target: Target, pipeline: Pipeline[_]) {
        pipeline.jpipeline.write(collection.native, target)
      }
    }
  }

  /**
   * PReaders are used to perform the actual reading of sources.
   *
   * @tparam S The type of the source being read from.
   * @tparam C The type of the collections being read from the specified source.
   */
  trait PReader[S, C] {
    def read(source: S, pipeline: Pipeline[_]): C
  }

  /**
   * Companion object.
   */
  object PReader {
    /**
     * Creates a PReader that reads Sources by delegating to the crunch pipeline.
     */
    def SourceReader[T] = {
      new PReader[Source[T], PCollection[T]]() {
        def read(source: Source[T], pipeline: Pipeline[_]): PCollection[T] = {
          new PCollection[T](pipeline.jpipeline.read(source))
        }
      }
    }
  }
}
