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

import java.lang.Class

import org.apache.hadoop.conf.Configuration

import com.cloudera.crunch.{Pipeline => JPipeline}
import com.cloudera.crunch.impl.mem.MemPipeline
import com.cloudera.crunch.impl.mr.MRPipeline

/**
 * Manages the state of a pipeline execution.
 *
 * ==Overview==
 * There are two subtypes of [[com.cloudera.crunch.Pipeline]]:
 * [[com.cloudera.crunch.Pipeline#MapReduce]] - for jobs run on a Hadoop cluster.
 * [[com.cloudera.crunch.Pipeline#InMemory]] - for jobs run in memory.
 *
 * To create a Hadoop pipeline:
 * {{{
 * import com.cloudera.scrunch.Pipeline
 *
 * Pipeline.mapreduce[MyClass]
 * }}}
 *
 * To get an in memory pipeline:
 * {{{
 * import com.cloudera.scrunch.Pipeline
 *
 * Pipeline.inMemory
 * }}}
 */
class Pipeline(val jpipeline: JPipeline) extends PipelineLike {
  /**
   * A convenience method for reading a text file.
   *
   * @param pathName Path to desired text file.
   * @return A PCollection containing the lines in the specified file.
   */
  def readTextFile(pathName: String): PCollection[String] = {
    new PCollection[String](jpipeline.readTextFile(pathName))
  }

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
 * Companion object. Contains subclasses of Pipeline.
 */
object Pipeline {
  /**
   * Pipeline for running jobs on a hadoop cluster.
   *
   * @param clazz Type of the class using the pipeline.
   * @param configuration Hadoop configuration to use.
   */
  class MapReducePipeline (clazz: Class[_], configuration: Configuration)
    extends Pipeline(new MRPipeline(clazz, configuration))

  /**
   * Pipeline for running jobs in memory.
   */
  object InMemoryPipeline extends Pipeline(MemPipeline.getInstance())

  /**
   * Creates a pipeline for running jobs on a hadoop cluster using the default configuration.
   *
   * @param clazz Type of the class using the pipeline.
   */
  def mapReduce(clazz: Class[_]): MapReducePipeline = mapReduce(clazz, new Configuration())

  /**
   * Creates a pipeline for running jobs on a hadoop cluster.
   *
   * @param clazz Type of the class using the pipeline.
   * @param configuration Hadoop configuration to use.
   */
  def mapReduce(clazz: Class[_], configuration: Configuration): MapReducePipeline = {
    new MapReducePipeline(clazz, configuration)
  }

  /**
   * Creates a pipeline for running jobs on a hadoop cluster using the default configuration.
   *
   * @tparam T Type of the class using the pipeline.
   */
  def mapReduce[T : ClassManifest]: MapReducePipeline = mapReduce[T](new Configuration())

  /**
   * Creates a pipeline for running jobs on a hadoop cluster.
   *
   * @param configuration Hadoop configuration to use.
   * @tparam T Type of the class using the pipeline.
   */
  def mapReduce[T : ClassManifest](configuration: Configuration): MapReducePipeline = {
    new MapReducePipeline(implicitly[ClassManifest[T]].erasure, configuration)
  }

  /**
   * Gets a pipeline for running jobs in memory.
   */
  def inMemory: InMemoryPipeline.type = InMemoryPipeline

  /**
   * Creates a new Pipeline according to the provided specifications.
   *
   * @param configuration Configuration for connecting to a Hadoop cluster.
   * @param memory Option specifying whether or not the pipeline is an in memory or mapreduce pipeline.
   * @param manifest ClassManifest for the class using the pipeline.
   * @tparam T type of the class using the pipeline.
   * @deprecated Use either {{{Pipeline.mapReduce(class, conf)}}} or {{{Pipeline.inMemory}}}
   */
  def apply[T](
    configuration: Configuration = new Configuration(),
    memory: Boolean = false)(implicit manifest: ClassManifest[T]
  ): Pipeline = if (memory) inMemory else mapReduce(manifest.erasure, configuration)
}
