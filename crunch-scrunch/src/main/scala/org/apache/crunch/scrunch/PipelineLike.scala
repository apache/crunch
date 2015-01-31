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
package org.apache.crunch.scrunch

import org.apache.hadoop.conf.Configuration

import org.apache.crunch.{Pipeline => JPipeline, _}
import org.apache.crunch.scrunch.interpreter.InterpreterRunner
import org.apache.crunch.types.{PTableType, PType}

import scala.collection.JavaConversions
import scala.collection.JavaConversions.asJavaCollection

trait PipelineLike {
  def jpipeline: JPipeline

  // Call this to ensure we set this up before any subsequent calls to the system
  PipelineLike.setupConf(getConfiguration())

  /**
   * Gets the configuration object associated with this pipeline.
   */
  def getConfiguration(): Configuration = jpipeline.getConfiguration()

  /**
   * Sets the configuration object associated with this pipeline.
   */
  def setConfiguration(conf: Configuration) {
    jpipeline.setConfiguration(conf)
  }

  /**
   * Returns the name of this pipeline instance.
   */
  def getName() = jpipeline.getName()

  /**
   * Reads a source into a [[org.apache.crunch.scrunch.PCollection]]
   *
   * @param source The source to read from.
   * @tparam T The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[T](source: Source[T]): PCollection[T] = new PCollection(jpipeline.read(source))

  /**
   * Reads a source into a [[org.apache.crunch.scrunch.PCollection]]
   *
   * @param source The source to read from.
   * @param named A short name to use for the returned PCollection.
   * @tparam T The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[T](source: Source[T], named: String): PCollection[T] = new PCollection(jpipeline.read(source, named))

  /**
   * Reads a source into a [[org.apache.crunch.scrunch.PTable]]
   *
   * @param source The source to read from.
   * @tparam K The type of the keys being read.
   * @tparam V The type of the values being read.
   * @return A PCollection containing data read from the specified source.
   */
  def read[K, V](source: TableSource[K, V]): PTable[K, V] = new PTable(jpipeline.read(source))

  /**
   * Reads a source into a [[org.apache.crunch.scrunch.PTable]]
   *
   * @param source The source to read from.
   * @param named A short name to use for the return PTable.
   * @tparam K The type of the keys being read.
   * @tparam V The type of the values being read.
   * @return A PTable containing data read from the specified source.
   */
  def read[K, V](source: TableSource[K, V], named: String): PTable[K, V] = new PTable(jpipeline.read(source, named))

  /**
   * Writes a parallel collection to a target.
   *
   * @param collection The collection to write.
   * @param target The destination target for this write.
   */
  def write(collection: PCollection[_], target: Target): Unit = jpipeline.write(collection.native, target)

  /**
   * Writes a parallel collection to a target using an output strategy.
   *
   * @param collection The collection to write.
   * @param target The destination target for this write.
   * @param writeMode The WriteMode to use for handling existing outputs.
   */
  def write(collection: PCollection[_], target: Target, writeMode: Target.WriteMode): Unit = {
    jpipeline.write(collection.native, target, writeMode)
  }

  /**
   * Writes a parallel table to a target.
   *
   * @param table The table to write.
   * @param target The destination target for this write.
   */
  def write(table: PTable[_, _], target: Target): Unit = jpipeline.write(table.native, target)

  /**
   * Writes a parallel table to a target.
   *
   * @param table The table to write.
   * @param target The destination target for this write.
   * @param writeMode The write mode to use on the target
   */
  def write(table: PTable[_, _], target: Target, writeMode: Target.WriteMode): Unit = {
    jpipeline.write(table.native, target, writeMode)
  }

  /**
   * Creates an empty PCollection of the given PType.
   */
  def emptyPCollection[T](pt: PType[T]) = new PCollection[T](jpipeline.emptyPCollection(pt))

  /**
   * Creates an empty PTable of the given PTableType.
   */
  def emptyPTable[K, V](pt: PTableType[K, V]) = new PTable[K, V](jpipeline.emptyPTable(pt))

  /**
   * Creates a new PCollection from the given elements.
   */
  def create[T](elements: Iterable[T], pt: PType[T]) = {
    new PCollection[T](jpipeline.create(asJavaCollection(elements), pt))
  }

  /**
   * Creates a new PCollection from the given elements.
   */
  def create[T](elements: Iterable[T], pt: PType[T], options: CreateOptions) = {
    new PCollection[T](jpipeline.create(asJavaCollection(elements), pt, options))
  }

  /**
   *  Creates a new PTable from the given elements.
   */
  def create[K, V](elements: Iterable[(K, V)], pt: PTableType[K, V]) = {
    new PTable[K, V](jpipeline.create(asJavaCollection(elements.map(t => Pair.of(t._1, t._2))), pt))
  }

  /**
   *  Creates a new PTable from the given elements.
   */
  def create[K, V](elements: Iterable[(K, V)], pt: PTableType[K, V], options: CreateOptions) = {
    new PTable[K, V](jpipeline.create(asJavaCollection(elements.map(t => Pair.of(t._1, t._2))), pt, options))
  }

  /**
   * Creates a new PCollection as the union of the given elements.
   */
  def union[S](elements: Seq[PCollection[S]]) = {
    val natives = elements.map(pc => pc.native)
    val jpc = jpipeline.union(JavaConversions.seqAsJavaList(natives))
    new PCollection[S](jpc)
  }

  /**
   * Creates a new PTable as the union of the given elements.
   */
  def unionTables[K, V](elements: Seq[PTable[K, V]]) = {
    val natives = elements.map(pc => pc.native)
    val jpt = jpipeline.unionTables(JavaConversions.seqAsJavaList(natives))
    new PTable[K, V](jpt)
  }

  /**
   * Adds the given {@code SeqDoFn} to the pipeline execution and returns its output.
   */
  def sequentialDo[Output](seqDoFn: PipelineCallable[Output]) = jpipeline.sequentialDo(seqDoFn)

  /**
   * Returns a handler for controlling the execution of the underlying MapReduce
   * pipeline.
   */
  def runAsync(): PipelineExecution = {
    PipelineLike.setupConf(getConfiguration())
    jpipeline.runAsync()
  }

  /**
   * Constructs and executes a series of MapReduce jobs in order
   * to write data to the output targets.
   */
  def run(): PipelineResult = {
    PipelineLike.setupConf(getConfiguration())
    jpipeline.run()
  }

  /**
   * Run any remaining jobs required to generate outputs and then
   * clean up any intermediate data files that were created in
   * this run or previous calls to `run`.
   */
  def done(): PipelineResult =  {
    PipelineLike.setupConf(getConfiguration())
    jpipeline.done()
  }

  /**
   * Cleans up any artifacts created as a result of {@link #run() running} the pipeline.
   *
   * @param force forces the cleanup even if all targets of the pipeline have not been completed.
   */
  def cleanup(force: Boolean): Unit = {
    jpipeline.cleanup(force)
  }

  /**
   * Turn on debug logging for jobs that are run from this pipeline.
   */
  def debug(): Unit = jpipeline.enableDebug()
}

object PipelineLike {
  def setupConf(conf: Configuration) {
    InterpreterRunner.addReplJarsToJob(conf)
    if (conf.get("crunch.reflectdatafactory", "").isEmpty) {
      // Enables the Scala-specific ReflectDataFactory
      conf.set("crunch.reflectdatafactory", classOf[ScalaReflectDataFactory].getCanonicalName)
    }
  }
}